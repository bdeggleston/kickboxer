package partitioner

type Token []byte

type Partitioner interface {
	GetToken(key string) Token
}
