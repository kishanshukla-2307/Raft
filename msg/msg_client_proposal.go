package msg

type MsgClientProposal struct {
	Command []byte
}

type MsgClientProposalResponse struct {
	Response string
	Success  bool
}
