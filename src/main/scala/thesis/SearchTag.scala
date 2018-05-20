package thesis

class SearchTag (
  val tag: String,
  val operator: String,
  val value: String
){
  override def toString: String = {
    return "["+tag+" "+operator+" "+value+"]"
  }
}

object SearchTag{

}
