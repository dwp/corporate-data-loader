package app.load.mapreduce

import app.load.utility.Converter
import app.load.utility.MessageParser
import com.nhaarman.mockitokotlin2.*
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.equality.shouldBeEqualToUsingFields
import io.kotest.matchers.shouldBe
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import java.text.SimpleDateFormat

class UcMapperTest : StringSpec({

    "Does shit" {

        val date = "2020-09-22T23:16:34.260+0000"

        val json = """
            {
                "message": {
                    "_id": {
                        "id": "1234567"
                    },
                    "_lastModifiedDateTime": "$date"
                }
            }
        """.trimIndent()

        val body = "${json}XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX".toByteArray()
        val key = mock<LongWritable>()
        val value = mock<Text> {
            on { bytes } doReturn body
            on { length } doReturn json.length
        }

        val context =
                mock<Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue>.Context>()
        val mapper = UcMapper()
        mapper.map(key, value, context)

        val convertor = Converter()
        val parser = MessageParser()

        val (_, id) = parser.generateKeyFromRecordBody(convertor.convertToJson(json.toByteArray()))

        val version = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZZZZ").parse(date).time

        val idCaptor = argumentCaptor<ImmutableBytesWritable>()
        val bodyCaptor = argumentCaptor<KeyValue>()
        verify(context, times(1)).write(idCaptor.capture(), bodyCaptor.capture())
        verifyNoMoreInteractions(context)
        idCaptor.firstValue.get() shouldBe id
        with (bodyCaptor.firstValue) {
            timestamp shouldBe version
            this.qualifier shouldBe "record".toByteArray()
            this.family shouldBe "cf".toByteArray()
            this.value shouldBe json.toByteArray()
        }
    }
})
