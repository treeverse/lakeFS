package parade

type HandlerResult struct {
	Status     string
	StatusCode TaskStatusCodeValue
}

type TaskHandler interface {
	Handle(action string, body *string) HandlerResult
	Actions() []string
	Actor() ActorID
}
