# STOR Go Client

Go client for [STOR](https://github.com/cfichtmueller/stor)

## Install

`go get github.com/cfichtmueller/stor-go-client`

## Usage

```go
opts := stor.NewClientOptions().
    SetHost("http://localhost:8000").
    SetApiKey("s3cr3t")

storClient := stor.NewClient(opts)

err := storClient.CreateBucket(context.Background(), "my-first-bucket")
objects, err := storClient.List(context.Background(), "my-first-bucket", "", 1000)
```