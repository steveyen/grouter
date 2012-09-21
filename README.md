grouter - next-gen memcached/couchbase protocol router
======================================================

Instead of "hello world", I write proxy/routers when trying out a new
language.  With grouter, I can also play with design ideas for a moxi
"2.0", but learn golang at the same time.

Building
--------

First, set up your GOPATH, like...

    export GOPATH=~/go

Then, get the code...

    go get github.com/steveyen/grouter

Or, old-school...

    mkdir -p ~/go/src/github.com/steveyen
    cd ~/go/src/github.com/steveyen
    git clone git://github.com/steveyen/grouter.git

Or, just use 'go get github.com/steveyen/grouter'.

For develoeprs, to (re-)build it...

    cd grouter
    go build ./main/grouter.go

Running
-------

    ./grouter --help

License
-------

Apache 2.0 License
