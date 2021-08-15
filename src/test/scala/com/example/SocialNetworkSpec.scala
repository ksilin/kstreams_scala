package com.example

import org.apache.kafka.streams.{
  KeyValue,
  TestInputTopic,
  TestOutputTopic,
  Topology,
  TopologyTestDriver
}
import org.apache.kafka.streams.scala.ImplicitConversions._
import com.goyeau.kafka.streams.circe.CirceSerdes
import com.goyeau.kafka.streams.circe.CirceSerdes._
import io.circe._
import io.circe.syntax._
import io.circe.generic.auto._
import io.circe.generic.semiauto._
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.scala.kstream.{ KStream, KTable, Materialized }
import org.apache.kafka.streams.state.KeyValueStore

import java.net.URI
import java.time.Instant
import scala.collection.immutable
import scala.jdk.CollectionConverters._
import scala.util.{ Random, Try }

class SocialNetworkSpec extends SpecBase {

  val postTopicName             = "posts"
  val userTopicName             = "users"
  val likesTopicName            = "likes"
  val commentTopicName          = "comments"
  val denormalizedPostTopicName = "denormalizedPosts"

  // without codecs for Instant & URI, compilation failed with: could not find implicit value for parameter e: io.circe.Encoder[com.example.Post]

  // semiauto did not work either, prob bcc of same missing codecs:
  // import io.circe._, io.circe.generic.semiauto._
  // implicit val fooDecoder: Decoder[Foo] = deriveDecoder[Foo]
  // TIL: if auto-derivation fails, try providing explicit Codecs for inner classes
  // https://circe.github.io/circe/codecs/custom-codecs.html
  // https://www.scala-exercises.org/circe/Custom%20codecs

  implicit val encodeInstant: Encoder[Instant] = Encoder.encodeString.contramap[Instant](_.toString)

  implicit val decodeInstant: Decoder[Instant] = Decoder.decodeString.emap { str =>
    Try(Instant.parse(str)).toEither.left.map(_.getMessage)
  }

  implicit val encodeUri: Encoder[URI] =
    Encoder.encodeString.contramap[URI](_.toString)

  implicit val decodeUri: Decoder[URI] =
    Decoder.decodeString.emap { str =>
      try Right(URI.create(str))
      catch {
        case ex: Throwable => Left(ex.getMessage)
      }
    }

  // custom KeyCodec required to be able to decode Map.empty[Id[Post], Post]
  implicit val encoderPostId: KeyEncoder[Id[Post]] = new KeyEncoder[Id[Post]] {
    override def apply(id: Id[Post]): String = id.value
  }
  implicit val decoderPostId: KeyDecoder[Id[Post]] = new KeyDecoder[Id[Post]] {
    override def apply(key: String): Option[Id[Post]] = Some(Id[Post](key))
  }

  private val userId1: Id[User] = Id[User]("user1")
  private val userId2: Id[User] = Id[User]("user2")
  private val userId3: Id[User] = Id[User]("user3")

  private val now: Instant = Instant.now

  val users = List(
    User(
      id = userId1,
      updatedOn = now,
      nickname = "Vlad Islav",
      verified = true,
    ),
    User(
      id = userId2,
      updatedOn = now,
      nickname = "Volker Racho",
      verified = true,
    ),
    User(
      id = userId2,
      updatedOn = now,
      nickname = "Hue Mann",
      verified = true,
    )
  )

  private val postId1: Id[Post] = Id[Post]("post1")
  private val postId2: Id[Post] = Id[Post]("post2")
  private val postId3: Id[Post] = Id[Post]("post3")

  val posts = List(
    Post(
      id = postId1,
      updatedOn = now,
      author = userId1,
      text = Random.alphanumeric.take(10).mkString,
    ),
    Post(
      id = postId1,
      updatedOn = now.minusSeconds(10),
      author = userId1,
      text = Random.alphanumeric.take(10).mkString,
    ),
    Post(
      id = postId2,
      updatedOn = now,
      author = userId1,
      text = Random.alphanumeric.take(10).mkString,
    ),
    Post(
      id = postId3,
      updatedOn = now,
      author = userId2,
      text = Random.alphanumeric.take(10).mkString,
    )
  )
  val likes = List(
    Like(userId1, postId3, now),
    Like(userId2, postId1, now),
    Like(userId3, postId1, now),
  )
  val comments = List(
    Comment(
      id = Id[Comment]("comment1"),
      postId = postId1,
      updatedOn = now,
      author = userId3,
      text = Random.alphanumeric.take(10).mkString
    )
  )

  // topo

  val postsStream: KStream[Id[Post], Post] = builder.stream[Id[Post], Post](postTopicName)
  val usersStream: KStream[Id[User], User] = builder.stream[Id[User], User](userTopicName)
  val commentsStream: KStream[Id[Post], Comment] =
    builder.stream[Id[Post], Comment](commentTopicName)
  val likeStream: KStream[Id[Post], Like] = builder.stream[Id[Post], Like](likesTopicName)

  // interesting - this is how to get latest by an inner timestamp:
  val latestUserTable: KTable[Id[User], User] = usersStream.groupByKey.reduce((first, second) =>
    if (first.updatedOn.isAfter(second.updatedOn)) first else second
  )

  // associative & commutative
  val postsByAuthorMap: KTable[Id[User], Map[Id[Post], Post]] = postsStream
    .groupBy((_, post) => post.author)
    .aggregate(Map.empty[Id[Post], Post])(
      (_, post: Post, postAggregate: immutable.Map[Id[Post], Post]) =>
        // do not modify aggregation if it already contains later posts with same ID
        if (postAggregate.get(post.id).exists(_.updatedOn.isAfter(post.updatedOn))) postAggregate
        else postAggregate + (post.id -> post)
    )(
      // could not find implicit value for parameter materialized:
      // org.apache.kafka.streams.scala.kstream.Materialized[com.example.Id[com.example.User],scala.collection.immutable.Map[com.example.Id[com.example.Post],com.example.Post],org.apache.kafka.streams.scala.ByteArrayKeyValueStore]
      //    Map.empty[Id[Post], Post])(

      // could not find implicit value for evidence parameter of type io.circe.Encoder[Map[com.example.Id[com.example.Post],com.example.Post]]
      //      CirceSerdes.serde[Map[Id[Post], Post]]
      Materialized.`with`[Id[User], Map[Id[Post], Post], KeyValueStore[Bytes, Array[Byte]]](
        CirceSerdes.serde[Id[User]],
        CirceSerdes.serde[Map[Id[Post], Post]]
      )
    )
  val postsByAuthor: KTable[Id[User], Set[Post]] = postsByAuthorMap.mapValues(_.values.toSet)

  // associative
  val likesByPost: KTable[Id[Post], Set[Like]] =
    likeStream.groupByKey.aggregate(Set.empty[Like])((_, like, likeAggregate) =>
      if (like.unliked) likeAggregate - like else likeAggregate + like //(
    // works without an explicit Materialize
    // Materialized.`with`[Id[Post], Set[Like], KeyValueStore[Bytes, Array[Byte]]](
    // CirceSerdes.serde[Id[Post]],
    // CirceSerdes.serde[Set[Like]]
    )

  // not idempotent, since comments with same ID may have duplicates - user-triggered or technical dupes
  // val commentCountByPost: KTable[Id[Post], Int] = commentsStream.groupByKey.aggregate(0)((_, comment: Comment, commentCount: Int) =>
  //  if (comment.deleted) commentCount - 1 else commentCount + 1)

  // idempotent
  val commentCountByPost: KTable[Id[Post], Int] = commentsStream.groupByKey
    .aggregate(Set.empty[Id[Comment]])((_, comment, commentIds) =>
      if (comment.deleted) commentIds - comment.id else commentIds + comment.id
    )
    .mapValues(_.size)

  // Commutativity, Associativity and Idempotence => Semilattice

  // creating denormalized posts for reading
  val postsByUserWithAuthor: KTable[Id[User], Set[DenormalisedPost]] =
    postsByAuthor.join(latestUserTable)((posts: Set[Post], author: User) =>
      posts.map(p => DenormalisedPost(p, author, Interactions.EMPTY))
    )

  val postsByUserFlat: KStream[Id[User], DenormalisedPost] =
    postsByUserWithAuthor.toStream.flatMapValues(posts => posts)
  val denormalizedPostsByPostIdDeduped: KTable[Id[Post], DenormalisedPost] = postsByUserFlat
    .groupBy((_, denormalisedPost) => denormalisedPost.post.id)
    .reduce((left, right) => if (left.post.updatedOn.isAfter(right.post.updatedOn)) left else right)

  val withLikes: KTable[Id[Post], DenormalisedPost] =
    denormalizedPostsByPostIdDeduped.leftJoin(likesByPost)((post, l: Set[Like]) =>
      Option(l).fold(post)(likes => post.copy(interactions = post.interactions.copy(likes = likes)))
    )

  val withLikesAndCommentCount: KTable[Id[Post], DenormalisedPost] =
    withLikes.leftJoin(commentCountByPost)((post, cCount) =>
      Option(cCount).fold(post)(c => post.copy(interactions = post.interactions.copy(comments = c)))
    )

  // write out
  withLikesAndCommentCount.toStream.to(denormalizedPostTopicName)

  // --- topo

  // test driver setup

  val topology: Topology = builder.build()
  info(topology.describe())

  val topologyTestDriver = new TopologyTestDriver(topology, streamsConfiguration)

  val postsTopic: TestInputTopic[Id[Post], Post] = topologyTestDriver.createInputTopic(
    postTopicName,
    CirceSerdes.serializer[Id[Post]],
    CirceSerdes.serializer[Post]
  )

  val usersTopic: TestInputTopic[Id[User], User] = topologyTestDriver.createInputTopic(
    userTopicName,
    CirceSerdes.serializer[Id[User]],
    CirceSerdes.serializer[User]
  )
  println("usersTopic: ")
  println(usersTopic)

  val likesTopic: TestInputTopic[Id[Post], Like] = topologyTestDriver.createInputTopic(
    likesTopicName,
    CirceSerdes.serializer[Id[Post]],
    CirceSerdes.serializer[Like]
  )
  println("likesTopic: ")
  println(likesTopic)

  val commentTopic: TestInputTopic[Id[Post], Comment] = topologyTestDriver.createInputTopic(
    commentTopicName,
    CirceSerdes.serializer[Id[Post]],
    CirceSerdes.serializer[Comment]
  )

  val outputTopic: TestOutputTopic[Id[Post], DenormalisedPost] =
    topologyTestDriver.createOutputTopic(
      denormalizedPostTopicName,
      CirceSerdes.deserializer[Id[Post]],
      CirceSerdes.deserializer[DenormalisedPost]
    )

  usersTopic.pipeKeyValueList(users.map(u => new KeyValue(u.id, u)).asJava)
  postsTopic.pipeKeyValueList(posts.map(p => new KeyValue(p.id, p)).asJava)
  likesTopic.pipeKeyValueList(likes.map(l => new KeyValue(l.postId, l)).asJava)
  commentTopic.pipeKeyValueList(comments.map(c => new KeyValue(c.postId, c)).asJava)

  val results: List[KeyValue[Id[Post], DenormalisedPost]] =
    outputTopic.readKeyValuesToList().asScala.toList

  results foreach { kv =>
   // val vStr: String = kv.value.asJson.noSpacesSortKeys
    println(s"${kv.value.post.id}: " + kv.value.interactions)
    }

  //
//  Id(post1): Interactions(Set(),0)
//  Id(post1): Interactions(Set(),0)
//  Id(post1): Interactions(Set(),0)
//  Id(post2): Interactions(Set(),0)
//  Id(post3): Interactions(Set(),0)
//  Id(post3): Interactions(Set(Like(Id(user1),Id(post3),2021-08-14T14:52:54.432490Z,false)),0)
//  Id(post1): Interactions(Set(Like(Id(user2),Id(post1),2021-08-14T14:52:54.432490Z,false)),0)
//  Id(post1): Interactions(Set(Like(Id(user2),Id(post1),2021-08-14T14:52:54.432490Z,false), Like(Id(user3),Id(post1),2021-08-14T14:52:54.432490Z,false)),0)
//  Id(post1): Interactions(Set(Like(Id(user2),Id(post1),2021-08-14T14:52:54.432490Z,false), Like(Id(user3),Id(post1),2021-08-14T14:52:54.432490Z,false)),1)

  // we get all the changes in the output topic:
  // enablling caching and increasing commit interval does not help
//  KeyValue(Id(post1), DenormalisedPost(Post(Id(post1),2021-08-14T14:40:06.209501Z,Id(user1),kcSL4v4xxy,false),User(Id(user1),2021-08-14T14:40:06.209501Z,Vlad Islav,true,false),Interactions(Set(),0)))
//  KeyValue(Id(post1), DenormalisedPost(Post(Id(post1),2021-08-14T14:40:06.209501Z,Id(user1),kcSL4v4xxy,false),User(Id(user1),2021-08-14T14:40:06.209501Z,Vlad Islav,true,false),Interactions(Set(),0)))
//  KeyValue(Id(post1), DenormalisedPost(Post(Id(post1),2021-08-14T14:40:06.209501Z,Id(user1),kcSL4v4xxy,false),User(Id(user1),2021-08-14T14:40:06.209501Z,Vlad Islav,true,false),Interactions(Set(),0)))
//  KeyValue(Id(post2), DenormalisedPost(Post(Id(post2),2021-08-14T14:40:06.209501Z,Id(user1),b6CsNO43x2,false),User(Id(user1),2021-08-14T14:40:06.209501Z,Vlad Islav,true,false),Interactions(Set(),0)))
//  KeyValue(Id(post3), DenormalisedPost(Post(Id(post3),2021-08-14T14:40:06.209501Z,Id(user2),u2Unt2TUAg,false),User(Id(user2),2021-08-14T14:40:06.209501Z,Hue Mann,true,false),Interactions(Set(),0)))
//  KeyValue(Id(post3), DenormalisedPost(Post(Id(post3),2021-08-14T14:40:06.209501Z,Id(user2),u2Unt2TUAg,false),User(Id(user2),2021-08-14T14:40:06.209501Z,Hue Mann,true,false),Interactions(Set(Like(Id(user1),Id(post3),2021-08-14T14:40:06.209501Z,false)),0)))
//  KeyValue(Id(post1), DenormalisedPost(Post(Id(post1),2021-08-14T14:40:06.209501Z,Id(user1),kcSL4v4xxy,false),User(Id(user1),2021-08-14T14:40:06.209501Z,Vlad Islav,true,false),Interactions(Set(Like(Id(user2),Id(post1),2021-08-14T14:40:06.209501Z,false)),0)))
//  KeyValue(Id(post1), DenormalisedPost(Post(Id(post1),2021-08-14T14:40:06.209501Z,Id(user1),kcSL4v4xxy,false),User(Id(user1),2021-08-14T14:40:06.209501Z,Vlad Islav,true,false),Interactions(Set(Like(Id(user2),Id(post1),2021-08-14T14:40:06.209501Z,false), Like(Id(user3),Id(post1),2021-08-14T14:40:06.209501Z,false)),0)))
//  KeyValue(Id(post1), DenormalisedPost(Post(Id(post1),2021-08-14T14:40:06.209501Z,Id(user1),kcSL4v4xxy,false),User(Id(user1),2021-08-14T14:40:06.209501Z,Vlad Islav,true,false),Interactions(Set(Like(Id(user2),Id(post1),2021-08-14T14:40:06.209501Z,false), Like(Id(user3),Id(post1),2021-08-14T14:40:06.209501Z,false)),1)))


}
