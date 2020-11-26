package parade

type ActorResult struct {
	Status     string
	StatusCode TaskStatusCodeValue
}

// Actor handles an action or a group of actions
type Actor interface {
	// Handle performs actions with the given body and return the ActorResult
	Handle(action string, body *string, signalledErrors int) ActorResult
	// Actions returns the list of actions that could be performed by the Actor
	Actions() []string
	// ActorID returns the ID of the actor
	ActorID() ActorID
}
