def printRightAngleTriangle(rows: Int): Unit = {
  for (i <- 1 to rows) {
    for (j <- 1 to i) {
      print("*")
    }
    println()
  }
}

printRightAngleTriangle(5)


------------Python------------
def printRightAngleTriangle(rows):
  for i in range(1, rows + 1):
    for j in range(1, i + 1):
      print("*", end="")
    print()

printRightAngleTriangle(5)
