require 'rubygems'
require 'uri'
require 'net/http'
require 'json'

class ClientException < Exception
  attr_reader :scheme, :host, :port, :path, :query, :status, :reason, :devices
  def initialize(msg, params={})
    @msg     = msg
    @scheme  = params[:http_scheme]
    @host    = params[:http_host]
    @port    = params[:http_port]
    @path    = params[:http_path]
    @query   = params[:http_query]
    @status  = params[:http_status]
    @reason  = params[:http_reason]
    @device  = params[:http_device]
    
    def to_s
      a = @msg
      b = ''
      if @scheme
        b += "#{@scheme}://"
      end
      if @host
        b += @host
      end
      if @port
        b +=  ":#{@port}"
      end
      if @path
        b += @path
      end
      if @query
        b += "?#{@query}"
      end
      if @status
        if b
          b = "#{b} #{@status}"
        else
          b = @status.to_s
        end
      end
      if @reason
        if b
          b = "#{b} #{@reason}"
        else
          b = "- #{@reason}"
        end
      end
      if @device
        if b
          b = "#{b}: device #{@device}"
        else
          b = "device #{@device}"
        end
      end
      b ? "#{a} #{b}" : a
    end
  end
end

def quote(value)
  URI.encode(value)
end

def http_connection(url)
  parsed = URI::parse(url)
  conn = Net::HTTP.new(parsed.host, parsed.port)
  if parsed.scheme == 'http'
    [parsed, conn]
  elsif parsed.scheme == 'https'
    conn.use_ssl = true
    conn.verify_mode = OpenSSL::SSL::VERIFY_NONE
    [parsed, conn]
  else
    raise ClientException.new("Cannot handle protocol scheme #{parsed.scheme} for #{url} %s")
  end
end

def get_auth(url, user, key, snet=false)
  parsed, conn = http_connection(url)
  conn.start
  resp = conn.get(parsed.request_uri, { "x-auth-user" => user, "x-auth-key" => key })
  
  if resp.code.to_i < 200 or resp.code.to_i > 300
    raise ClientException
  end
  url = URI::parse(resp.header['x-storage-url'])
  if snet:
    url.host = "snet-#{url.host}"
  end
  [url.to_s, resp.header['x-auth-token']]
end

def get_account(url, token, marker=nil, limit=nil, prefix=nil, http_conn=nil, full_listing=false)
  #todo: add in rest of functionality
  if not http_conn
    http_conn = http_connection(url)
  end
  parsed, conn = http_conn
  if full_listing
    rv = get_account(url, token, marker, limit, prefix, http_conn)
    listing = rv[1]
    while listing.length > 0
      marker = listing[-1]['name']
      listing = get_account(url, token, marker, limit, prefix, http_conn)[1]
      if listing.length > 0
        rv[1] << listing
      end
    end
    return rv
  end
  if parsed.query == nil
    parsed.query = "format=json"
  else
    parsed.query += "&format=json"
  end
  if marker:
    parsed.query += "&marker=#{quote(marker.to_s)}"
  end
  if limit:
    parsed.query += "&limit=#{quote(limit.to_s)}"
  end
  if prefix:
    parsed.query += "&prefix=#{quote(prefix.to_s)}"
  end
  conn.start if conn.started?
  puts parsed.request_uri
  resp = conn.get(parsed.request_uri, {'x-auth-token' => token})
  resp_headers = {}
  resp.header.each do |k,v|
    resp_headers[k.downcase] = v
  end
  if resp.code.to_i < 200 or resp.code.to_i > 300
    raise ClientException.new('Account GET failed', :http_scheme=>parsed.scheme,
                :http_host=>conn.address, :http_port=>conn.port,
                :http_path=>parsed.path, :http_query=>parsed.query, :http_status=>resp.code,
                :http_reason=>resp.message)
  end
  if resp.code.to_i == 204
    [resp_headers, []]
  else
    [resp_headers, JSON.parse(resp.body)]
  end
end

def head_account(url, token, http_conn=nil)
  if not http_conn
    http_conn = http_connection(url)
  end
  parsed, conn = http_conn
  conn.start
  resp = conn.head(parsed.request_uri, {'x-auth-token' => token})
  resp_headers = {}
  resp.header.each do |k,v|
    resp_headers[k.downcase] = v
  end
  if resp.code.to_i < 200 or resp.code.to_i > 300
    raise ClientException.new('Account HEAD failed', :http_scheme=>parsed.scheme,
                :http_host=>conn.address, :http_port=>conn.port,
                :http_path=>parsed.path, :http_status=>resp.code,
                :http_reason=>resp.message)
  end
  resp_headers
end

def post_account(url, token, headers, http_conn=nil)
  if not http_conn
    http_conn = http_connection(url)
  end
  parsed, conn = http_conn
  headers['x-auth-token'] = token
  conn.start
  resp = conn.post(parsed.request_uri, nil, headers)
  resp.body
  if resp.code.to_i < 200 or resp.code.to_i > 300
    raise ClientException.new('Account POST failed', :http_scheme=>parsed.scheme,
                :http_host=>conn.address, :http_port=>conn.port,
                :http_path=>parsed.path, :http_status=>resp.code,
                :http_reason=>resp.message)
  end
end

def get_container(url, token, container, marker=nil, limit=nil, prefix=nil, delimiter=nil, http_conn=nil, full_listing=nil)
  #todo: add in rest of functionality
  if not http_conn
    http_conn = http_connection(url)
  end
  parsed, conn = http_conn
  
  if parsed.query == nil
    parsed.query = "format=json"
  else
    parsed.query += "&format=json"
  end
  conn.start
  parsed.path += "/#{quote(container)}"
  resp = conn.get(parsed.request_uri, {'x-auth-token' => token})
  resp_headers = {}
  resp.header.each do |k,v|
    resp_headers[k.downcase] = v
  end
  if resp.code.to_i < 200 or resp.code.to_i > 300
    raise ClientException.new('Container GET failed', :http_scheme=>parsed.scheme,
                :http_host=>conn.address, :http_port=>conn.port,
                :http_path=>parsed.path, :http_query=>parsed.query, :http_status=>resp.code,
                :http_reason=>resp.message)
  end
  if resp.code.to_i == 204:
    [resp_headers, []]
  else
    [resp_headers, JSON.parse(resp.body())]
  end
end

def head_container(url, token, container, http_conn=nil)
  if not http_conn
    http_conn = http_connection(url)
  end
  parsed, conn = http_conn
  conn.start
  parsed.path += "/#{quote(container)}"
  resp = conn.head(parsed.request_uri, {'x-auth-token' => token})
  resp_headers = {}
  resp.header.each do |k,v|
    resp_headers[k.downcase] = v
  end
  if resp.code.to_i < 200 or resp.code.to_i > 300
    raise ClientException.new('Container HEAD failed', :http_scheme=>parsed.scheme,
                :http_host=>conn.address, :http_port=>conn.port,
                :http_path=>parsed.path, :http_status=>resp.code,
                :http_reason=>resp.message)
  end
  resp_headers
end

def put_container(url, token, container, headers={}, http_conn=nil)
  if not http_conn
    http_conn = http_connection(url)
  end
  parsed, conn = http_conn
  conn.start
  parsed.path += "/#{quote(container)}"
  headers['x-auth-token'] = token
  resp = conn.put(parsed.request_uri, nil, headers)
  if resp.code.to_i < 200 or resp.code.to_i > 300
    raise ClientException.new('Container PUT failed', :http_scheme=>parsed.scheme,
                :http_host=>conn.address, :http_port=>conn.port,
                :http_path=>parsed.path, :http_status=>resp.code,
                :http_reason=>resp.message)  
  end
end

def post_container(url, token, container, headers={}, http_conn=nil)
  if not http_conn
    http_conn = http_connection(url)
  end
  parsed, conn = http_conn
  conn.start
  parsed.path += "/#{quote(container)}"
  headers['x-auth-token'] = token
  resp = conn.post(parsed.request_uri, nil, headers)
  if resp.code.to_i < 200 or resp.code.to_i > 300
    raise ClientException.new('Container POST failed', :http_scheme=>parsed.scheme,
                :http_host=>conn.address, :http_port=>conn.port,
                :http_path=>parsed.path, :http_status=>resp.code,
                :http_reason=>resp.message)
  end
end

def delete_container(url, token, container, http_conn=nil)
  if not http_conn
    http_conn = http_connection(url)
  end
  parsed, conn = http_conn
  conn.start
  parsed.path += "/#{quote(container)}"
  resp = conn.delete(parsed.request_uri, {'x-auth-token' => token})
  if resp.code.to_i < 200 or resp.code.to_i > 300
    raise ClientException.new('Container DELETE failed', :http_scheme=>parsed.scheme,
                :http_host=>conn.address, :http_port=>conn.port,
                :http_path=>parsed.path, :http_status=>resp.code,
                :http_reason=>resp.message)
  end
end