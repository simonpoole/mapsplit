### Value encoding
    
    6                 4                   3    2 2 22
    3                 8                   2    8 7 54
    XXXX XXXX XXXX XXXX YYYY YYYY YYYY YYYY 1uuu uNNE nnnn nnnn nnnn nnnn nnnn nnnn

    X - tile number
    Y - tile number
    u - unused
    1 - always set to 1. This ensures that the value can be distinguished from empty positions in an array.
    N - bits indicating immediate "neigbours"
    E - extended "neighbour" list used
    n - bits for "short" neighbour index

    Tiles indexed in "short" list (T original tile)
          -  - 
          2  1  0  1  2
        
    -2    0  1  2  3  4
    -1    5  6  7  8  9
     0   10 11  T 12 13
     1   14 15 16 17 18
     2   19 20 21 22 23
     
 
