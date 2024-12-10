def printPyramid(rows: Int): Unit = {
  for (i <- 1 to rows) {
    for (j <- 1 to rows - i) {
      print(" ")
    }
    for (k <- 1 to 2 * i - 1) {
      print("*")
    }
    println()
  }
}

printPyramid(5)
def printInvertedPyramid(rows: Int): Unit = {
  for (i <- rows to 1 by -1) {
    for (j <- 1 to rows - i) {
      print(" ")
    }
    for (k <- 1 to 2 * i - 1) {
      print("*")
    }
    println()
  }
}
def printDiamond(rows: Int): Unit = {
  printPyramid(rows)
  printInvertedPyramid(rows - 1)
}

printDiamond(5)

def print_diamond(rows: Int): Unit = {
  for (i <- 1 to rows) {
    // Print spaces
    for (j <- 1 to rows - i) {
      print(" ")
    }
    // Print stars
    for (k <- 1 to 2 * i - 1) {
      print("*")
    }
    println()
  }

  for (i <- rows - 1 to 1 by -1) {
    // Print spaces
    for (j <- 1 to rows - i) {
      print(" ")
    }
    // Print stars
    for (k <- 1 to 2 * i - 1) {
      print("*")
    }
    println()
  }
}

print_diamond(5)
------------Python------------
def printPyramid(rows):
  for i in range(1, rows + 1):
    for j in range(1, rows - i + 1):
      print(" ", end="")
    for k in range(1, 2 * i):
      print("*", end="")
    print()

print_pyramid(5)
def printInvertedPyramid(rows):
  for i in range(rows, 0, -1):
    for j in range(1, rows - i + 1):
      print(" ", end="")
    for k in range(1, 2 * i):
      print("*", end="")
    print()

print_inverted_pyramid(5)
def printDiamond(rows):
  printPyramid(rows)
  printInvertedPyramid(rows - 1)

printDiamond(5)

def print_diamond(rows):
    for i in range(1, rows + 1):
        # Print spaces
        for j in range(1, rows - i + 1):
            print(" ", end="")
        # Print stars
        for k in range(1, 2 * i):
            print("*", end="")
        print()

    for i in range(rows - 1, 0, -1):
        # Print spaces
        for j in range(1, rows - i + 1):
            print(" ", end="")
        # Print stars
        for k in range(1, 2 * i):
            print("*", end="")
        print()


rows = 5
print_diamond(rows)
