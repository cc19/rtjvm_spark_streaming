package part1recap

object ScalaRecap {

  //values and variables
  val aBoolena:Boolean = false

  //expressions
  val anExpression = if (2>3) "Bigger" else "Smaller"

  //instructions vs expressions
  /*
  instructions are executed one by one. A program is a sequence of instructions.
  expressions are evaluated, i.e they can be reduced to a single value
   */
  val theUnit = println("Hello World")  //returns Unit = no meaningful value, void in other languages

  //functions
  def myFunction(x: Int) = 42   //42 is the whole expression which is the implementation of this function

  //OOP

  class Animal
  class Dog extends Animal
  trait Carnivore {
    def eat(animal :Animal) :Unit
  }

  class Crocodile extends Animal with Carnivore{
    override def eat(animal: Animal) :Unit = println("crunch!")
  }

  //singleton pattern
  object MySingleTon

  //companions
  object Carnivore  //same name as a class

  //generics
  trait MyList[A]

  //method notation
  val x =1 + 2
  val y = 1.+(2)

}
