module github.com/marie20767/web-crawler/services/initialiser

go 1.25.0

require (
	github.com/google/uuid v1.6.0
	github.com/marie20767/web-crawler/shared v0.0.0-00010101000000-000000000000
	github.com/segmentio/kafka-go v0.4.50
)

replace github.com/marie20767/web-crawler/shared => ../../shared

require (
	github.com/joho/godotenv v1.5.1 // indirect
	github.com/klauspost/compress v1.15.9 // indirect
	github.com/pierrec/lz4/v4 v4.1.15 // indirect
	golang.org/x/text v0.36.0 // indirect
)
