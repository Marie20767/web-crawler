package message

// ParserMessage is the value produced to the parser Kafka topic.
type ParserMessage struct {
	PageURL    string `json:"page_url"`
	StorageURL string `json:"storage_url"`
}
