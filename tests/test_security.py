from apps.api.security import hash_password, hash_session_token, new_session_token, verify_password


def test_password_hash_verifies_and_rejects_wrong_password() -> None:
    password_hash = hash_password("correct horse battery staple")

    assert verify_password("correct horse battery staple", password_hash)
    assert not verify_password("wrong password", password_hash)


def test_session_tokens_are_random_and_hashed() -> None:
    token_a = new_session_token()
    token_b = new_session_token()

    assert token_a != token_b
    assert hash_session_token(token_a) == hash_session_token(token_a)
    assert hash_session_token(token_a) != token_a
