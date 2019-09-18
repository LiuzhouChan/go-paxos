# Example 1 - Hello World #

## First Run ##
Start three instances of the helloworld program on the same machine in three different terminals:

```
./example-helloworld -nodeid 1
```
```
./example-helloworld -nodeid 2
```
```
./example-helloworld -nodeid 3
```


## Restart a node ##
Let's pick a stopped instance and restart it using the exact same command, e.g. for the one which we previously started with the *-nodeid 2* command line option, it can be restarted using the command below - 
```
./example-helloworld -nodeid 2
```
