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
  end
      
  def to_s
    a = @msg
    b = ''
    b += "#{@scheme}://" if @scheme
    b += @host if @host
    b +=  ":#{@port}" if @port
    b += @path if @path
    b += "?#{@query}" if @query
    b ? b = "#{b} #{@status}" : b = @status.to_s if @status
    b ? b = "#{b} #{@reason}" : b = "- #{@reason}" if @reason
    b ? b = "#{b}: device #{@device}" : b = "device #{@device}" if @device
    b ? "#{a} #{b}" : a
  end
end

class ChunkedConnectionWrapper
  def initialize(data, chunk_size)
    @size = chunk_size
    if data.respond_to? :read
      @file = data
    end
  end
  
  def read(foo)
    if @file
      @file.read(@size)
    end
  end
  def eof!
    @file.eof!
  end
  def eof?
    @file.eof?
  end
end

def quote(value)
  URI.encode(value)
end

class Query
  def initialize(url_params)
    if url_params
      @params = Query.from_url_params(url_params)
    else
      @params = {}
    end
  end
  def to_s
    to_url_params
  end
  def to_url_params
    elements = []
    @params.each_pair {|k,v| elements << "#{k}=#{v}"}
    elements.join('&')
  end
  def self.from_url_params(url_params)
    result = {}
    url_params.split('&').each do |element|
      element = element.split('=')
      result[element[0]] = element[1]
    end
    result
  end
  def has_key?(key)
    @params.has_key? key
  end
  def add(key, value)
    @params[key] = value
  end
  def delete(key)
    @params.delete(key)
  end
end

class Connection
  def initialize(authurl, user, key, retries=5, preauthurl=nil, preauthtoken=nil, snet=false, starting_backoff=1)
    @authurl = authurl
    @user = user
    @key = key
    @retries = retries
    @http_conn = nil
    @url = preauthurl
    @token = preauthtoken
    @attempts = 0
    @snet = snet
    @starting_backoff = starting_backoff
  end
  
private
  def _retry(reset, func, args=nil, opts={})
    @url, @token = self.get_auth() if !@url or !@token
    @http_conn = self.http_connection() if !@http_conn
    args.unshift(@url, @token)
    Connection.method(func).call(*args)
  end

public
  def self.http_connection(url)
    parsed = URI::parse(url)
    conn = Net::HTTP.new(parsed.host, parsed.port)
    if parsed.scheme == 'http'
      [parsed, conn]
    elsif parsed.scheme == 'https'
      conn.use_ssl = true
      conn.verify_mode = OpenSSL::SSL::VERIFY_NONE
      [parsed, conn]
    else
      raise ClientException.new(
        "Cannot handle protocol scheme #{parsed.scheme} for #{url} %s")
    end
  end
  
  def http_connection
    if !@http_conn
      @http_conn = Connection.http_connection(@url)
    else
      @http_conn
    end
  end
  
  def self.get_auth(url, user, key, snet=false)
    parsed, conn = http_connection(url)
    conn.start if !conn.started?
    resp = conn.get(parsed.request_uri, 
      { "x-auth-user" => user, "x-auth-key" => key })

    if resp.code.to_i < 200 or resp.code.to_i > 300
      raise ClientException
    end
    url = URI::parse(resp.header['x-storage-url'])
    if snet:
      url.host = "snet-#{url.host}"
    end
    [url.to_s, resp.header['x-auth-token']]
  end
  
  def get_auth
    @url, @token = Connection.get_auth(@authurl, @user, @key, @snet)
  end
  
  def self.get_account(url, token, marker=nil, limit=nil, prefix=nil, 
      http_conn=nil, full_listing=false)
    #todo: add in rest of functionality
    if not http_conn
      http_conn = http_connection(url)
    end
    parsed = http_conn[0].clone
    conn = http_conn[1]
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
    query = Query.new(parsed.query)
    query.add('format', 'json')
    query.add('marker', quote(marker.to_s)) if marker
    query.add('limit', quote(limit.to_s)) if limit
    query.add('prefix', quote(prefix.to_s)) if prefix
    parsed.query = query.to_url_params
    conn.start if !conn.started?
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
  
  def get_account(marker=nil, limit=nil, prefix=nil, full_listing=false)
    _retry(nil, :get_account, [marker, limit, prefix, @http_conn, full_listing])
  end
      
  def self.head_account(url, token, http_conn=nil)
    if not http_conn
      http_conn = http_connection(url)
    end
    parsed = http_conn[0].clone
    conn = http_conn[1]
    conn.start if !conn.started?
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
  
  def head_account
    _retry(nil, :head_account, [@http_conn])
  end

  def self.post_account(url, token, headers, http_conn=nil)
    if not http_conn
      http_conn = http_connection(url)
    end
    parsed = http_conn[0].clone
    conn = http_conn[1]
    headers['x-auth-token'] = token
    conn.start if !conn.started?
    resp = conn.post(parsed.request_uri, nil, headers)
    resp.body
    if resp.code.to_i < 200 or resp.code.to_i > 300
      raise ClientException.new('Account POST failed', :http_scheme=>parsed.scheme,
              :http_host=>conn.address, :http_port=>conn.port,
              :http_path=>parsed.path, :http_status=>resp.code,
              :http_reason=>resp.message)
    end
  end
  def post_account(headers=nil)
    _retry(nil, :head_account, [headers, @http_conn])
  end

  def self.get_container(url, token, container, marker=nil, limit=nil, 
        prefix=nil, delimiter=nil, http_conn=nil, full_listing=nil)
    #todo: add in rest of functionality
    if not http_conn
      http_conn = http_connection(url)
    end
    parsed = http_conn[0].clone
    conn = http_conn[1]
    
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
    query = Query.new(parsed.query)
    query.add('format', 'json')
    query.add('marker', quote(marker.to_s)) if marker
    query.add('limit', quote(limit.to_s)) if limit
    query.add('prefix', quote(prefix.to_s)) if prefix
    parsed.query = query.to_url_params
    conn.start if !conn.started?
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
  
  def get_container(container, marker=nil, limit=nil, prefix=nil, delimiter=nil, full_listing=nil)
    _retry(nil, :get_container, [container, marker, limit, prefix, delimiter, full_listing])
  end

  def self.head_container(url, token, container, http_conn=nil)
    if not http_conn
      http_conn = http_connection(url)
    end
    parsed = http_conn[0].clone
    conn = http_conn[1]
    
    conn.start if !conn.started?
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
  
  def head_container(container)
    _retry(nil, :head_container, [container, @http_conn])
  end

  def self.put_container(url, token, container, headers={}, http_conn=nil)
    if not http_conn
      http_conn = http_connection(url)
    end
    parsed = http_conn[0].clone
    conn = http_conn[1]
    
    conn.start if !conn.started?
    parsed.path += "/#{quote(container)}"
    headers['x-auth-token'] = token
    # headers['content-length'] = 0
    resp = conn.put(parsed.request_uri, nil, headers)
    if resp.code.to_i < 200 or resp.code.to_i > 300
      raise ClientException.new('Container PUT failed', :http_scheme=>parsed.scheme,
                  :http_host=>conn.address, :http_port=>conn.port,
                  :http_path=>parsed.path, :http_status=>resp.code,
                  :http_reason=>resp.message)  
    end
  end
  
  def put_container(container, headers={})
    _retry(nil, :put_container, [container, headers, @http_conn])
  end

  def self.post_container(url, token, container, headers={}, http_conn=nil)
    if not http_conn
      http_conn = http_connection(url)
    end
    parsed = http_conn[0].clone
    conn = http_conn[1]
    
    conn.start if !conn.started?
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
  
  def post_container(container, headers={})
    _retry(nil, :post_container, [container, headers])
  end

  def self.delete_container(url, token, container, http_conn=nil)
    if not http_conn
      http_conn = http_connection(url)
    end
    parsed = http_conn[0].clone
    conn = http_conn[1]
    
    conn.start if !conn.started?
    parsed.path += "/#{quote(container)}"
    resp = conn.delete(parsed.request_uri, {'x-auth-token' => token})
    if resp.code.to_i < 200 or resp.code.to_i > 300
      raise ClientException.new('Container DELETE failed', :http_scheme=>parsed.scheme,
                  :http_host=>conn.address, :http_port=>conn.port,
                  :http_path=>parsed.path, :http_status=>resp.code,
                  :http_reason=>resp.message)
    end
  end
  
  def delete_container(container)
    _retry(nil, :delete_container, [container])
  end

  def self.get_object(url, token, container, name, http_conn=nil, resp_chunk_size=nil)
    if not http_conn
      http_conn = http_connection(url)
    end
    parsed = http_conn[0].clone
    conn = http_conn[1]
    

    parsed.path += "/#{quote(container)}/#{quote(name)}"
    conn.start if not conn.started?
    resp = conn.get(parsed.request_uri, {'x-auth-token' => token})
    if resp.code.to_i < 200 or resp.code.to_i > 300
      raise ClientException.new('Object GET failed', :http_scheme=>parsed.scheme,
                  :http_host=>conn.address, :http_port=>conn.port,
                  :http_path=>parsed.path, :http_status=>resp.code,
                  :http_reason=>resp.message)
    end

    if resp_chunk_size
      #todo: finish this out.
    else
      object_body = resp.body  
    end
    resp_headers = {}
    resp.header.each do |k,v|
      resp_headers[k.downcase] = v
    end
    [resp_headers, object_body]
  end

  def self.head_object(url, token, container, name, http_conn=nil)
    if not http_conn
      http_conn = http_connection(url)
    end
    parsed = http_conn[0].clone
    conn = http_conn[1]
    

    parsed.path += "/#{quote(container)}/#{quote(name)}"
    conn.start if not conn.started?
    resp = conn.head(parsed.request_uri, {'x-auth-token' => token})
    if resp.code.to_i < 200 or resp.code.to_i > 300
      raise ClientException.new('Object HEAD failed', :http_scheme=>parsed.scheme,
                  :http_host=>conn.address, :http_port=>conn.port,
                  :http_path=>parsed.path, :http_status=>resp.code,
                  :http_reason=>resp.message)
    end
    resp_headers = {}
    resp.header.each do |k,v|
      resp_headers[k.downcase] = v
    end
    resp_headers
  end

  def self.put_object(url, token=nil, container=nil, name=nil, contents=nil,
                 content_length=nil, etag=nil, chunk_size=65536,
                 content_type=nil, headers={}, http_conn=nil, proxy=nil)
    if not http_conn
      http_conn = http_connection(url)
    end
    parsed = http_conn[0].clone
    conn = http_conn[1]
                  
    parsed.path += "/#{quote(container)}" if container
    parsed.path += "/#{quote(name)}" if name
    headers['x-auth-token'] = token if token
    headers['etag'] = etag if etag
    if content_length != nil
      headers['content-length'] = content_length.to_s
    else
      headers.each do |k,v|
        if k.downcase == 'content-length'
          content_length = v.to_i
        end
      end
    end
    headers['content-type'] = content_type if content_type
    headers['content-length'] = '0' if not contents  
    if contents.respond_to? :read
      request = Net::HTTP::Put.new(parsed.request_uri, headers)
      chunked = ChunkedConnectionWrapper.new(contents, chunk_size)
      if content_length == nil
        request['Transfer-Encoding'] = 'chunked'
        request.delete('content-length')
      end
      request.body_stream = chunked
      resp = conn.start do |http|
        http.request(request)
      end
    else
      resp = conn.put(parsed.request_uri, contents, headers)
    end
    if resp.code.to_i < 200 or resp.code.to_i > 300
      raise ClientException.new('Object PUT failed', :http_scheme=>parsed.scheme,
                  :http_host=>conn.address, :http_port=>conn.port,
                  :http_path=>parsed.path, :http_status=>resp.code,
                  :http_reason=>resp.message)
    end
    resp.header['etag']
  end

  def self.post_object(url, token=nil, container=nil, name=nil, headers={}, http_conn=nil)
    if not http_conn
       http_conn = http_connection(url)
    end
    parsed = http_conn[0].clone
    conn = http_conn[1]
    
    parsed.path += "/#{quote(container)}" if container
    parsed.path += "/#{quote(name)}" if name
    headers['x-auth-token'] = token if token
    resp = conn.post(parsed.request_uri, nil, headers)
    if resp.code.to_i < 200 or resp.code.to_i > 300
      raise ClientException.new('Object POST failed', :http_scheme=>parsed.scheme,
                  :http_host=>conn.address, :http_port=>conn.port,
                  :http_path=>parsed.path, :http_status=>resp.code,
                  :http_reason=>resp.message)
    end
  end

  def self.delete_object(url, token=nil, container=nil, name=nil, http_conn=nil, headers={}, proxy=nil)
    if not http_conn
      http_conn = http_connection(url)
    end
    parsed = http_conn[0].clone
    conn = http_conn[1]
    
    conn.start if !conn.started?
    parsed.path += "/#{quote(container)}" if container
    parsed.path += "/#{quote(name)}" if name
    headers['x-auth-token'] = token if token
    resp = conn.delete(parsed.request_uri, headers)
    if resp.code.to_i < 200 or resp.code.to_i > 300
      raise ClientException.new('Object DELETE failed', :http_scheme=>parsed.scheme,
                  :http_host=>conn.address, :http_port=>conn.port,
                  :http_path=>parsed.path, :http_status=>resp.code,
                  :http_reason=>resp.message)
    end
  end
end