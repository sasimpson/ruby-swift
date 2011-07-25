require 'client.rb'
require 'test/unit'

class TestClient < Test::Unit::TestCase
  
  def setup
    @url = "http://saio.scottic.us:8080/auth/v1.0"
    @user = "test:tester"
    @key = "testing"
    @auth = get_auth(@url, @user, @key)
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
    assert_equal(3, @auth.length)
  end
  
  def test_get_account
    account = get_account(@auth[0], @auth[1])
    assert_equal(2, account.length)
    assert_not_nil(account[0]['x-account-bytes-used'])
    assert_not_nil(account[0]['x-account-object-count'])
    assert_not_nil(account[0]['x-account-container-count'])
    assert_not_nil(account[0]['content-length'])
    assert_not_nil(account[0]['date'])
  end
  
  def test_head_account
    account = head_account(@auth[0], @auth[1])
    assert_not_nil(account['x-account-bytes-used'])
    assert_not_nil(account['x-account-object-count'])
    assert_not_nil(account['x-account-container-count'])
    assert_not_nil(account['content-length'])
    assert_not_nil(account['date'])
  end
  
  def test_post_account
    post_account(@auth[0], @auth[1], {'x-account-meta-test-header' => 'test header'})
    account = get_account(@auth[0], @auth[1])
    assert_equal('test header', account[0]['x-account-meta-test-header'])
  end
  
  def test_get_container
    assert_raise ClientException do 
      container = get_container(@auth[0], @auth[1], 'no-container')
    end
  end
end