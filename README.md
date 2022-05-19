# Bully Leader Election Algorithm

Distributed leader election algorithm where alive node with highest nodeID becomes the leader

## To run

In path/to/bully execute the following command with your parameters

```sh
python bully.py --numproc <numproc(int)> --numstarter <numstarter(int)> --numalive <numalive(int)>
```

# Constraints for the parameters
- NumProc > NumAlive
- NumAlive > NumStarter