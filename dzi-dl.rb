#!/usr/bin/env ruby
# frozen_string_literal: true

require 'nokogiri'
require 'open-uri'
require 'open_uri_redirections'
require 'tempfile'
require 'robotex'
require 'uri'
require 'json'
require 'net/https'
require 'ruby-progressbar'
require 'shellwords'
require 'addressable/uri'
require 'fileutils'
require 'thread'
require 'optparse'

USER_AGENT    = ENV['USER_AGENT'] || 'dzi-dl'
DEFAULT_DELAY = ENV['DEFAULT_DELAY'].nil? ? 1 : ENV['DEFAULT_DELAY'].to_f
MAX_RETRIES   = ENV['MAX_RETRIES'].nil? ? 3 : ENV['MAX_RETRIES'].to_i
VERIFY_SSL    = (ENV['VERIFY_SSL'] == 'true') ? OpenSSL::SSL::VERIFY_PEER : OpenSSL::SSL::VERIFY_NONE
OPEN_URI_OPTIONS = { "User-Agent" => USER_AGENT, allow_redirections: :all, ssl_verify_mode: VERIFY_SSL }

def do_mogrify(filename, tile_size, overlap, gravity)
  geometry = "#{tile_size}x#{tile_size}-#{overlap}-#{overlap}"
  `mogrify -gravity #{gravity} -crop #{geometry} +repage #{filename}`
  if $?.exitstatus != 0
    destination = File.join('backup', File.basename(filename))
    $stderr.puts "Error calling `mogrify` on #{filename}. Copying bad file to: #{destination}"
    FileUtils.mkdir_p('backup')
    FileUtils.cp(filename, destination, verbose: true)
    raise "Non-zero exit status for `mogrify`"
  end
end

def content_type_ok?(ct, fmt)
  return false if ct.nil?
  ct = ct.downcase
  case fmt.downcase
  when 'jpg', 'jpeg'
    ct.include?('image/jpeg') || ct.include?('image/jpg')
  when 'png'
    ct.include?('image/png')
  when 'webp'
    ct.include?('image/webp')
  else
    ct.start_with?('image/')
  end
end

def download_tile!(tile_url, tempfile_path, deepzoom_format, delay:, max_retries:)
  retries = 0
  begin
    URI.open(tile_url, OPEN_URI_OPTIONS) do |open_uri_response|
      ct = open_uri_response.meta['content-type']
      unless content_type_ok?(ct, deepzoom_format)
        raise "Got response content-type: #{ct}"
      end
      IO.copy_stream(open_uri_response, tempfile_path)
      raise "#{tempfile_path} doesn't exist" unless File.exist?(tempfile_path)
    end
  rescue StandardError => e
    if retries < max_retries
      retries += 1
      sleep(delay)
      retry
    end
    raise e
  end
end

# ---- CLI ----
threads = (ENV['THREADS'] || 4).to_i
OptionParser.new do |o|
  o.banner = "Usage: #{File.basename($PROGRAM_NAME)} [options] <dzi_or_xml_url>"
  o.on('-t', '--threads N', Integer, "Download threads (default: #{threads})") { |n| threads = n }
end.parse!
url = ARGV[0]
if url.nil? || url.strip.empty?
  $stderr.puts "ERROR: Missing DZI/XML URL"
  exit 1
end
threads = 1 if threads < 1

begin
  `montage --version`
  raise "Non-zero exit status" if $?.exitstatus != 0
rescue StandardError
  $stderr.puts "Unable to call `montage` command from ImageMagick.\nPlease ensure ImageMagick is installed and available on your PATH."
  exit 1
end

$stderr.puts "URL: #{url}"
files_url = url.sub(/\.(xml|dzi)$/, '_files')
$stderr.puts "DeepZoom files URL: #{files_url}"
$stderr.puts "Threads: #{threads}"

doc = Nokogiri::XML(URI.open(url)).remove_namespaces!
$stderr.puts "DZI XML document contents:"
$stderr.puts doc.to_s

deepzoom = {}
deepzoom[:tile_size] = doc.xpath('/Image/@TileSize').first.value.to_i
deepzoom[:overlap]   = doc.xpath('/Image/@Overlap').first.value.to_i
deepzoom[:format]    = doc.xpath('/Image/@Format').first.value
deepzoom[:width]     = doc.xpath('/Image/Size/@Width').first.value.to_i
deepzoom[:height]    = doc.xpath('/Image/Size/@Height').first.value.to_i
$stderr.puts "DeepZoom parameters:\n#{JSON.pretty_generate(deepzoom)}"

output_filename = url.split(/[?\/=]/).last.sub(/\.(xml|dzi)$/, '') + '.' + deepzoom[:format]

max_level = Math.log2([deepzoom[:width], deepzoom[:height]].max).ceil
$stderr.puts "#{max_level + 1} tile levels"

tiles_x = (deepzoom[:width].to_f / deepzoom[:tile_size]).ceil
tiles_y = (deepzoom[:height].to_f / deepzoom[:tile_size]).ceil
total_tiles = tiles_x * tiles_y
$stderr.puts "#{tiles_x} x #{tiles_y} = #{total_tiles} tiles"

progress_bar = ProgressBar.create(
  title: "Downloading Tiles",
  total: total_tiles,
  format: '%t (%c/%C): |%B| %p%% %E'
)

# ---- robots + delay ----
robotex = Robotex.new(USER_AGENT)
# 把 robots.txt 結果「快取」起來，避免多執行緒反覆抓
robots_cache = {}
robots_mutex = Mutex.new

allowed_and_delay = lambda do |tile_url|
  key = begin
    u = Addressable::URI.parse(tile_url)
    "#{u.scheme}://#{u.host}:#{u.port}"
  rescue
    tile_url
  end

  robots_mutex.synchronize do
    return robots_cache[key] if robots_cache.key?(key)
    allowed = robotex.allowed?(tile_url)
    delay   = robotex.delay(tile_url)
    robots_cache[key] = [allowed, delay]
  end
end

# ---- work queue ----
queue = Queue.new
tempfiles = Array.new(tiles_y) { Array.new(tiles_x) }

# 先建好所有 tempfile 與 job（避免 worker 還要做結構性工作）
(0...tiles_y).each do |y|
  (0...tiles_x).each do |x|
    tempfile = Tempfile.new(["#{x}_#{y}", ".#{deepzoom[:format]}"])
    tempfile.close
    tempfiles[y][x] = tempfile

    tile_url = Addressable::URI.normalized_encode(
      File.join(files_url, max_level.to_s, "#{x}_#{y}.#{deepzoom[:format]}")
    )
    queue << [x, y, tile_url, tempfile.path]
  end
end

# ---- parallel download ----
errors = Queue.new
inc_mutex = Mutex.new

workers = Array.new(threads) do
  Thread.new do
    loop do
      job = nil
      begin
        job = queue.pop(true)
      rescue ThreadError
        break
      end

      x, y, tile_url, path = job

      allowed, rdelay = allowed_and_delay.call(tile_url)
      unless allowed
        errors << "User agent \"#{USER_AGENT}\" not allowed by `robots.txt` for #{tile_url}"
        break
      end

      delay = (rdelay ? rdelay : DEFAULT_DELAY)

      begin
        download_tile!(tile_url, path, deepzoom[:format], delay: delay, max_retries: MAX_RETRIES)
        sleep(delay)
      rescue StandardError => e
        errors << "Tile #{x}_#{y} failed: #{e.inspect} (URL: #{tile_url})"
        break
      ensure
        inc_mutex.synchronize { progress_bar.increment }
      end
    end
  end
end

workers.each(&:join)

unless errors.empty?
  # 取第一個錯誤就結束（其餘也可以自行加總輸出）
  $stderr.puts "ERROR: #{errors.pop}"
  tempfiles.flatten.each { |t| t.unlink unless t.nil? }
  exit 1
end

begin
  if deepzoom[:overlap] != 0
    $stderr.puts "Shaving overlap from tiles"
    (0...tiles_x).each do |x|
      (0...tiles_y).each do |y|
        gravity = ''
        gravity += 'North' if y == 0
        gravity += 'South' if y == (tiles_y - 1)
        gravity += 'West'  if x == 0
        gravity += 'East'  if x == (tiles_x - 1)
        gravity = 'Center' if gravity.empty?
        do_mogrify(tempfiles[y][x].path, deepzoom[:tile_size], deepzoom[:overlap], gravity)
      end
    end
  end

  $stderr.puts "Combining tiles into #{output_filename}"
  `montage -mode concatenate -tile #{tiles_x}x#{tiles_y} #{tempfiles.flatten.map { |t| t.path }.join(' ')} #{Shellwords.escape(output_filename)}`
  if $?.exitstatus != 0
    destination = Shellwords.escape(File.join('backup', File.basename(output_filename)))
    $stderr.puts "Error calling `montage` for #{output_filename}. Moving bad output to: #{destination}"
    FileUtils.mkdir_p('backup')
    FileUtils.mv(output_filename, destination, verbose: true, force: true)
    raise "Non-zero exit status for `montage`"
  end

  unless File.exist?(output_filename)
    $stderr.puts "ERROR: Expected #{output_filename} to be assembled from tiles, but file does not exist."
    exit 1
  end
rescue StandardError => e
  $stderr.puts("#{e.message}, exiting")
  exit 1
ensure
  tempfiles.flatten.each { |t| t.unlink unless t.nil? }
end
