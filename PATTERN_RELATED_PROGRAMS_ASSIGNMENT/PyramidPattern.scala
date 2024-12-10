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

------------Python------------
def printPyramid(rows):
  for i in range(1, rows + 1):
    for j in range(1, rows - i + 1):
      print(" ", end="")
    for k in range(1, 2 * i):
      print("*", end="")
    print()

printPyramid(5)
