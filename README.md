## Distributed Web Crawler

Implementation of a distributed web crawler using:
 - Kafka
 - Golang
 - Terraform

### App architecture
![high-level system architecture](apparchitecture.png)

### Development

#### Run with docker
```
make up
```

#### Run without docker
To run the initialiser|crawler, cd into the correct folder (i.e. `services/initialiser`|`crawler`) and run:
```
make run
```

Kafka is only run via docker. From `kafka/docker`, run:
```
make up
```

#### Lint
To lint the initialiser|crawler, cd into the correct folder (i.e. `services/initialiser`|`crawler`) and run:
```
make lint
```

or

```
make lint/fix
```
