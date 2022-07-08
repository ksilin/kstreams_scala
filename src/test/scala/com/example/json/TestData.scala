package com.example.json

import com.example.Translation

import java.util.regex.Pattern

case object TestData{

  val textLines = List(
    "this is the end",
  )

  val wordPattern: Pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS)

  val translationsEn: List[(String, String)] = List(
    ("this", "dies"),
    ("this", "es"),
    ("the", "der"),
    ("the", "die"),
    ("the", "das"),
    ("is", "ist"),
    ("end", "ende"),
    ("end", "schluss"),
  )

  val expectedTranslations = Map(
    "the"  -> Translation("the", Set("der", "die", "das")),
    "this" -> Translation("this", Set("dies", "es")),
    "is"   -> Translation("is", Set("ist")),
    "end"  -> Translation("end", Set("ende", "schluss"))
  )


}
