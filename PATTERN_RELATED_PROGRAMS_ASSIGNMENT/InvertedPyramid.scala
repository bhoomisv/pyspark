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

printInvertedPyramid(5)
------------Python------------
def printInvertedPyramid(rows):
  for i in range(rows, 0, -1):
    for j in range(1, rows - i + 1):
      print(" ", end="")
    for k in range(1, 2 * i):
      print("*", end="")
    print()

printInvertedPyramid(5)
