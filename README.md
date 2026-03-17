## Distributed Web Crawler

Implementation of a distributed web crawler using Golang and Kafka

### App architecture
![high-level system architecture](apparchitecture.png)

### Development

#### Prerequisite files:
- `.env`

#### Setup
```
make setup
```

#### Run with docker
```
make up
```

#### Run without docker
To run the go producer:
```
make run
```

Kafka is only run via docker. From `kafka/docker`, run:
```
make up
```

#### Lint
```
make lint
```

and

```
make lint/fix
```
