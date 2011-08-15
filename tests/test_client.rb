require '../client.rb'
require 'test/unit'

class TestClient < Test::Unit::TestCase
  def setup
    # if you want to run these functional tests (i know it says unit) you have 
    # to have a swift instance to test against.
    @url = "http://saio.local:8080/auth/v1.0"
    @user = "test:tester"
    @key = "testing"
    @storage_url, @auth_token = Connection.get_auth(@url, @user, @key)
    @file = File::open('/tmp/test.txt', 'w')
    @file.write((1..100).collect {('a'..'z').collect.join}.join)
    @file.close
    @file = File::open('/tmp/test.txt')
  end
  
  def test_http_connection
    parsed, conn = Connection.http_connection("http://localhost:8080/auth/v1.0")
    assert_equal('http', parsed.scheme)
    assert_equal('localhost', parsed.host)
    assert_equal(8080, parsed.port)
    assert_equal('/auth/v1.0', parsed.path)
    assert_nil(parsed.query)
  end
  
  def test_get_auth
    assert_not_nil(@storage_url)
    assert_not_nil(@auth_token)
  end
  
  def test_get_account
    account = Connection.get_account(@storage_url, @auth_token)
    assert_equal(2, account.length)
    assert_not_nil(account[0]['x-account-bytes-used'])
    assert_not_nil(account[0]['x-account-object-count'])
    assert_not_nil(account[0]['x-account-container-count'])
    assert_not_nil(account[0]['content-length'])
    assert_not_nil(account[0]['date'])
    
    (1..20).each {|n| Connection.put_container(@storage_url, @auth_token, "test_get_account_#{n}")}
    account = Connection.get_account(@storage_url, @auth_token, 'test_get_account_3', 1)
    assert_equal('test_get_account_4', account[1][0]['name'], "check that marker pulls next container")
    account = Connection.get_account(@storage_url, @auth_token, nil, 2)
    assert_equal(2, account[1].length, "check that limit properly limits the amount of containers returned")
    account = Connection.get_account(@storage_url, @auth_token, nil, nil, 'test_get_account_')
    assert_equal('test_get_account_1', account[1][0]['name'], "check prefix works")
    account = Connection.get_account(@storage_url, @auth_token, nil, nil, nil, nil, true)
    assert_equal('test_get_account_1', account[1][0]['name'], "check that full listing returns all data")
    assert (account[1].length >= 20)
    (1..20).each {|n| Connection.delete_container(@storage_url, @auth_token, "test_get_account_#{n}")}
  end
  
  def test_head_account
    account = Connection.head_account(@storage_url, @auth_token)
    assert_not_nil(account['x-account-bytes-used'])
    assert_not_nil(account['x-account-object-count'])
    assert_not_nil(account['x-account-container-count'])
    assert_not_nil(account['content-length'])
    assert_not_nil(account['date'])
  end
  
  def test_post_account
    Connection.post_account(@storage_url, @auth_token, {'x-account-meta-test-post-header' => 'test header'})
    account = Connection.get_account(@storage_url, @auth_token)
    assert_equal('test header', account[0]['x-account-meta-test-post-header'], "check that header is added to account")
    Connection.post_account(@storage_url, @auth_token, {'x-account-meta-test-post-header' => 'change test header'})
    account = Connection.get_account(@storage_url, @auth_token)
    assert_equal('change test header', account[0]['x-account-meta-test-post-header'], "check that the account header is changed")
    Connection.post_account(@storage_url, @auth_token, {'x-account-meta-test-post-header' => ''})
    account = Connection.get_account(@storage_url, @auth_token)
    assert_nil(account[0]['x-account-meta-test-post-header'], "check that the account header is removed.")
  end
  
  def test_get_container
    assert_raises ClientException do 
      container = Connection.get_container(@storage_url, @auth_token, 'no-container')
    end
    Connection.put_container(@storage_url, @auth_token, 'test_get_container', {'x-container-meta-get-container-header' => 'testing'})
    container = Connection.get_container(@storage_url, @auth_token, 'test_get_container')
    assert_equal(2, container.length)
    assert_equal('testing', container[0]['x-container-meta-get-container-header'])
    Connection.delete_container(@storage_url, @auth_token, 'test_get_container')
  end
  
  def test_head_container
    assert_raises ClientException do 
      container = Connection.head_container(@storage_url, @auth_token, 'no-container')
    end
    Connection.put_container(@storage_url, @auth_token, 'test_head_container', {'x-container-meta-head-container-header' => 'testing'})
    container = Connection.head_container(@storage_url, @auth_token, 'test_head_container')
    assert_not_nil(container)
    assert_equal('testing', container['x-container-meta-head-container-header'])
    Connection.delete_container(@storage_url, @auth_token, 'test_head_container')
  end
  
  def test_put_container
    Connection.put_container(@storage_url, @auth_token, 'test_put_container', {'x-container-meta-put-container-header' => 'testing'})
    container = Connection.get_container(@storage_url, @auth_token, 'test_put_container')
    assert_equal(2, container.length)
    assert_equal('testing', container[0]['x-container-meta-put-container-header'])
    Connection.delete_container(@storage_url, @auth_token, 'test_put_container')
  end
  
  def test_post_container
    Connection.put_container(@storage_url, @auth_token, 'test_post_container', {'x-container-meta-post-container-header' => 'testing'})
    Connection.post_container(@storage_url, @auth_token, 'test_post_container', {'x-container-meta-post-container-header' => 'changed'})
    container = Connection.head_container(@storage_url, @auth_token, 'test_post_container')
    assert_not_nil(container)
    assert_equal('changed', container['x-container-meta-post-container-header'])
    Connection.delete_container(@storage_url, @auth_token, 'test_post_container')
  end
  
  def test_delete_container
    Connection.put_container(@storage_url, @auth_token, 'test_delete_container')
    container = Connection.head_container(@storage_url, @auth_token, 'test_delete_container')
    assert_not_nil(container)
    Connection.delete_container(@storage_url, @auth_token, 'test_delete_container')
    assert_raises ClientException do
      container = Connection.head_container(@storage_url, @auth_token, 'test_delete_container')
    end
  end
  
  def test_object_opertations
    Connection.put_container(@storage_url, @auth_token, 'test_object')
    etag = Connection.put_object(@storage_url, @auth_token, 'test_object', 'test.txt', @file, nil, nil, 10, 'text/plain')
    obj = Connection.head_object(@storage_url, @auth_token, 'test_object', 'test.txt')
    assert_equal(etag, obj['etag'])
    Connection.post_object(@storage_url, @auth_token, 'test_object', 'test.txt', {'x-object-meta-post-object-header' => 'test'})
    obj = Connection.head_object(@storage_url, @auth_token, 'test_object', 'test.txt')
    assert_equal('test', obj['x-object-meta-post-object-header'])
    Connection.delete_object(@storage_url, @auth_token, 'test_object', 'test.txt')
    assert_raises ClientException do 
      obj = Connection.head_object(@storage_url, @auth_token, 'test_object', 'test.txt')
    end
    Connection.delete_container(@storage_url, @auth_token, 'test_object')
  end
  
  def test_oop_access
    swift = Connection.new(@url, @user, @key)
    (1..20).each {|n| swift.put_container("test_account_oop_#{n}")}
    account = swift.head_account
    assert_equal("20", account['x-account-container-count'], "head account returns correct number for x-account-container-count")
    account = swift.get_account(nil, nil, 'test_account_oop')
    assert_equal(20, account[1].length, "get returns the correct number of containers")
    swift.post_container('test_account_oop_1', {'x-container-meta-foo'=>'testing'})
    container = swift.head_container('test_account_oop_1')
    assert_equal('testing', container['x-container-meta-foo'], "post correctly set x-container-meta-foo")
    (1..20).each {|n| swift.delete_container("test_account_oop_#{n}")}
    swift.put_container('test_object_oop')
    @file.seek 0
    swift.put_object('test_object_oop', 'test.txt', @file)
    obj = swift.head_object('test_object_oop', 'test.txt')
    assert_equal(@file.stat.size, obj['content-length'])
  end
end
