package transport

type RegisterWorkerParam struct {
	ID   string
	Addr string
}

type RegisterWorkerReply struct {
	Success bool
}
