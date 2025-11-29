package coretype

type RegisterWorkerArgs struct {
	ID   string
	Addr string
}

type RegisterWorkerReply struct {
	Success bool
}
