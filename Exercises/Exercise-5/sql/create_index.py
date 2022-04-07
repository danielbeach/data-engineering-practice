AccountIndex = \
                """
                CREATE INDEX IF NOT EXISTS lastName_index ON accounts(customerId)
                """

ProductIndex = \
                """
                CREATE INDEX IF NOT EXISTS productId_index ON products(productId)
                """

TransactionIndex = \
                """
                CREATE INDEX IF NOT EXISTS transactionId_index ON transactions(transactionId)
                """