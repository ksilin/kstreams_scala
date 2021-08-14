package com

import java.net.URI
import java.time.Instant

package object example {

  case class Translation(orig: String, translations: Set[String])

  case class Id[Resource](value: String) extends AnyVal

  // write model
  sealed case class User(
      id: Id[User],
      updatedOn: Instant,
      nickname: String,
      verified: Boolean,
    //  image: Option[URI] = None,
      deleted: Boolean = false
  )

  sealed case class Post(
      id: Id[Post],
      updatedOn: Instant,
      author: Id[User],
      text: String,
      // image: Option[URI] = None,
      deleted: Boolean = false
  )
  sealed case class Like(userId: Id[User], postId: Id[Post], updatedOn: Instant, unliked: Boolean = false)
  sealed case class Comment(
      id: Id[Comment],
      postId: Id[Post],
      updatedOn: Instant,
      author: Id[User],
      text: String,
      deleted: Boolean = false
  )

  // read model
  sealed case class DenormalisedPost(post: Post, author: User, interactions: Interactions)
  sealed case class Interactions(likes: Set[Like], comments: Int){
  }
  case object Interactions{
    final val EMPTY = Interactions(Set.empty, 0)
  }

}
