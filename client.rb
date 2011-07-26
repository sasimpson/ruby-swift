require 'rubygems'
require 'uri'
require 'net/http'
require 'json'

class ClientException<Exception
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
    raise ClientException
  end
end

def get_auth(url, user, key, snet=false)
  parsed, conn = http_connection(url)
  conn.start
  resp = conn.get(parsed.request_uri, { "x-auth-user" => user, "x-auth-key" => key })
  
  if resp.code.to_i < 200 or resp.code.to_i > 300
    raise ClientException
  end
  url = resp.header['x-storage-url']
  #todo: snet
  
  [url, resp.header['x-storage-token'], resp.header['x-auth-token']]
end

def get_account(url, token, marker=nil, limit=nil, prefix=nil, http_conn=nil, full_listing=false)
  if not http_conn
    http_conn = http_connection(url)
  end
  parsed, conn = http_conn
  if parsed.query == nil
    parsed.query = "format=json"
  else
    parsed.query = "#{parsed.query}&format=json"
  end
  conn.start
  resp = conn.get(parsed.request_uri, {'x-auth-token' => token})
  resp_headers = {}
  resp.header.each do |k,v|
    resp_headers[k.downcase] = v
  end
  if resp.code.to_i < 200 or resp.code.to_i > 300
    #todo: add more exception info
    raise ClientException
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
    #todo: add more exception info
    raise ClientException
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
    #todo: add more exception info
    raise ClientException
  end
end

def get_container(url, token, container, marker=nil, limit=nil, prefix=nil, delimiter=nil, http_conn=nil, full_listing=nil)
  if not http_conn
    http_conn = http_connection(url)
  end
  parsed, conn = http_conn
  
  if parsed.query == nil
    parsed.query = "format=json"
  else
    parsed.query = "#{parsed.query}&format=json"
  end
  conn.start
  parsed.path += "/#{quote(container)}"
  resp = conn.get(parsed.request_uri, {'x-auth-token' => token})
  resp_headers = {}
  resp.header.each do |k,v|
    resp_headers[k.downcase] = v
  end
  if resp.code.to_i < 200 or resp.code.to_i > 300
    #todo: add more exception info
    raise ClientException
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
    #todo: add more exception info
    raise ClientException
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
    #todo: add more exception info
    raise ClientException
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
    #todo: add more exception info
    raise ClientException
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
    #todo: add more exception info
    raise ClientException
  end
end
