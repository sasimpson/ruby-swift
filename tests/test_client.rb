require '../client.rb'
require 'test/unit'

class TestClient < Test::Unit::TestCase
  def setup
    # if you want to run these functional tests (i know it says unit) you have 
    # to have a swift instance to test against.
    @url = "http://saio.local:8080/auth/v1.0"
    @user = "test:tester"
    @key = "testing"
    @storage_url, @auth_token = get_auth(@url, @user, @key)
    @file = File::open('/tmp/test.txt', 'w')
    @file.write((1..100).collect {('a'..'z').collect.join}.join)
    @file.close
    @file = File::open('/tmp/test.txt')
  end
  
  def test_http_connection
    parsed, conn = http_connection("http://localhost:8080/auth/v1.0")
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
    account = get_account(@storage_url, @auth_token)
    assert_equal(2, account.length)
    assert_not_nil(account[0]['x-account-bytes-used'])
    assert_not_nil(account[0]['x-account-object-count'])
    assert_not_nil(account[0]['x-account-container-count'])
    assert_not_nil(account[0]['content-length'])
    assert_not_nil(account[0]['date'])
    
    (1..20).each {|n| put_container(@storage_url, @auth_token, "test_get_account_#{n}")}
    account = get_account(@storage_url, @auth_token, 'test_get_account_3', 1)
    assert_equal('test_get_account_4', account[1][0]['name'], "check that marker pulls next container")
    account = get_account(@storage_url, @auth_token, nil, 2)
    assert_equal(2, account[1].length, "check that limit properly limits the amount of containers returned")
    account = get_account(@storage_url, @auth_token, nil, nil, 'test_get_account_')
    assert_equal('test_get_account_1', account[1][0]['name'], "check prefix works")
    account = get_account(@storage_url, @auth_token, nil, nil, nil, nil, true)
    assert_equal('test_get_account_1', account[1][0]['name'], "check that full listing returns all data")
    assert (account[1].length >= 20)
    (1..20).each {|n| delete_container(@storage_url, @auth_token, "test_get_account_#{n}")}
  end
  
  def test_head_account
    account = head_account(@storage_url, @auth_token)
    assert_not_nil(account['x-account-bytes-used'])
    assert_not_nil(account['x-account-object-count'])
    assert_not_nil(account['x-account-container-count'])
    assert_not_nil(account['content-length'])
    assert_not_nil(account['date'])
  end
  
  def test_post_account
    post_account(@storage_url, @auth_token, {'x-account-meta-test-post-header' => 'test header'})
    account = get_account(@storage_url, @auth_token)
    assert_equal('test header', account[0]['x-account-meta-test-post-header'], "check that header is added to account")
    post_account(@storage_url, @auth_token, {'x-account-meta-test-post-header' => 'change test header'})
    account = get_account(@storage_url, @auth_token)
    assert_equal('change test header', account[0]['x-account-meta-test-post-header'], "check that the account header is changed")
    post_account(@storage_url, @auth_token, {'x-account-meta-test-post-header' => ''})
    account = get_account(@storage_url, @auth_token)
    assert_nil(account[0]['x-account-meta-test-post-header'], "check that the account header is removed.")
  end
  
  def test_get_container
    assert_raises ClientException do 
      container = get_container(@storage_url, @auth_token, 'no-container')
    end
    put_container(@storage_url, @auth_token, 'test_get_container', {'x-container-meta-get-container-header' => 'testing'})
    container = get_container(@storage_url, @auth_token, 'test_get_container')
    assert_equal(2, container.length)
    assert_equal('testing', container[0]['x-container-meta-get-container-header'])
    delete_container(@storage_url, @auth_token, 'test_get_container')
  end
  
  def test_head_container
    assert_raises ClientException do 
      container = head_container(@storage_url, @auth_token, 'no-container')
    end
    put_container(@storage_url, @auth_token, 'test_head_container', {'x-container-meta-head-container-header' => 'testing'})
    container = head_container(@storage_url, @auth_token, 'test_head_container')
    assert_not_nil(container)
    assert_equal('testing', container['x-container-meta-head-container-header'])
    delete_container(@storage_url, @auth_token, 'test_head_container')
  end
  
  def test_put_container
    put_container(@storage_url, @auth_token, 'test_put_container', {'x-container-meta-put-container-header' => 'testing'})
    container = get_container(@storage_url, @auth_token, 'test_put_container')
    assert_equal(2, container.length)
    assert_equal('testing', container[0]['x-container-meta-put-container-header'])
    delete_container(@storage_url, @auth_token, 'test_put_container')
  end
  
  def test_post_container
    put_container(@storage_url, @auth_token, 'test_post_container', {'x-container-meta-post-container-header' => 'testing'})
    post_container(@storage_url, @auth_token, 'test_post_container', {'x-container-meta-post-container-header' => 'changed'})
    container = head_container(@storage_url, @auth_token, 'test_post_container')
    assert_not_nil(container)
    assert_equal('changed', container['x-container-meta-post-container-header'])
    delete_container(@storage_url, @auth_token, 'test_post_container')
  end
  
  def test_delete_container
    put_container(@storage_url, @auth_token, 'test_delete_container')
    container = head_container(@storage_url, @auth_token, 'test_delete_container')
    assert_not_nil(container)
    delete_container(@storage_url, @auth_token, 'test_delete_container')
    assert_raises ClientException do
      container = head_container(@storage_url, @auth_token, 'test_delete_container')
    end
  end
  
  def test_object
    put_container(@storage_url, @auth_token, 'test_object')
    etag = put_object(@storage_url, @auth_token, 'test_object', 'test.txt', @file, nil, nil, 10, 'text/plain')
    obj = head_object(@storage_url, @auth_token, 'test_object', 'test.txt')
    assert_equal(etag, obj['etag'])
    post_object(@storage_url, @auth_token, 'test_object', 'test.txt', {'x-object-meta-post-object-header' => 'test'})
    obj = head_object(@storage_url, @auth_token, 'test_object', 'test.txt')
    assert_equal('test', obj['x-object-meta-post-object-header'])
    delete_object(@storage_url, @auth_token, 'test_object', 'test.txt')
    assert_raises ClientException do 
      obj = head_object(@storage_url, @auth_token, 'test_object', 'test.txt')
    end
  end
end
