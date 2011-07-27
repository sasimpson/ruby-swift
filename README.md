ruby equivalent of client.py

examples:
=========
	@storage_url, @auth_token = get_auth('http://swift.example.com/auth/v1.0', 'user', 'key')
	@account = get_account(@storage_url, @auth_token)
	put_container(@storage_url, @auth_token, "new_container", {'x-container-meta-foo' => 'foobar'})
	@new_container = get_container(@storage_token, @auth_token, "new_container")
