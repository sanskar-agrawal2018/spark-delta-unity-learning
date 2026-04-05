package scalaLearning

object Solution {
  def findDifferentBinaryString(nums: Array[String],ans:String="",index:Int=0): String = {
    if(index==nums.length)  return ans

    def reverse(s:Char):Char={
      if(s=='0')   return '1'
      '0'
    }
    findDifferentBinaryString(nums,ans+reverse(nums(index)(index)),index+1)
  }
}


object test extends App{
  val nums=Array("00","01")
  println(Solution.findDifferentBinaryString(nums))
}

