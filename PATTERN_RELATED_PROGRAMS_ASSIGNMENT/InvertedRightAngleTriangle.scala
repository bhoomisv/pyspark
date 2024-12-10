
def printInvertedRightAngleTriangle(rows: Int): Unit = {
  for (i <- rows to 1 by -1) {
    for (j <- 1 to i) {
      print("*")
    }
    println()
  }
}

printInvertedRightAngleTriangle(5)
------------Python------------

def printInvertedRightAngleTriangle(rows):
  for i in range(rows, 0, -1):
    for j in range(1, i + 1):
      print("*", end="")
    print()

printInvertedRightAngleTriangle(5)
