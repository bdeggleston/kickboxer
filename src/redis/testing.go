package redis

// returns a redis instance with defaults set
func setupRedis() *Redis {
	return NewRedis()
}
