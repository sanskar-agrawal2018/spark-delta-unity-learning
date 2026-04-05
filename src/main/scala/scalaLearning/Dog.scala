package  scalaLearning.Animal
import scalaLearning.Animal.animal

class Dog extends animal {
  override def speak(): Unit = {
    println("The dog barks.")
  }

}
