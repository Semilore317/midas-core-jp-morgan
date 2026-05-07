package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.UserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;


@Component
public class KafkaConsumer {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);
    private final DatabaseConduit databaseConduit;
    private final UserRepository userRepository;

    public KafkaConsumer(DatabaseConduit databaseConduit, UserRepository userRepository) {
        this.databaseConduit = databaseConduit;
        this.userRepository = userRepository;
    }

    @KafkaListener(topics = "${general.kafka-topic}")
    public void listen(Transaction transaction) {
        logger.info("received: " + transaction);
        UserRecord sender = userRepository.findById(transaction.getSenderId());
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());
        TransactionRecord transactionRecord = new TransactionRecord(sender, recipient, transaction.getAmount());

        if (sender != null && recipient != null && sender.getBalance() >= transaction.getAmount()) {
            sender.setBalance(sender.getBalance() - transaction.getAmount());
            recipient.setBalance(recipient.getBalance() + transaction.getAmount());
            transactionRecord.setAmount(transaction.getAmount());
            transactionRecord.setRecipient(recipient);
            transactionRecord.setSender(sender);
            databaseConduit.save(sender);
            databaseConduit.save(recipient);
            databaseConduit.save(transactionRecord);
        }
    }
}
