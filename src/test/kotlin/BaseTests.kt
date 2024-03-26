
import kotlin.test.Test

import kotlin.test.assertNotEquals


class BaseTests {

        @Test
        fun `parse price point`() {
            // Given
            val receivedMessage = "3669.75;3670.25;2200.0;2400.0;1711448928527;0"
            // When
            val pp:PricePoint = deserializePricePoint(receivedMessage)
            // Then
            assert(pp.bid == 3669.75)
            assert(pp.ask == 3670.25)
            assert(pp.bidVolume == 2200.0)
            assert(pp.askVolume == 2400.0)
            assert(pp.epochSecond == 1711448928527)
            assert(pp.tsDiff == 0L)
        }

    }