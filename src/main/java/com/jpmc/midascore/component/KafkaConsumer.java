package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.UserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;


@Component
public class KafkaConsumer {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);
    private final DatabaseConduit databaseConduit;
    private final UserRepository userRepository;
    private final RestTemplate restTemplate;

    public KafkaConsumer(DatabaseConduit databaseConduit, UserRepository userRepository, RestTemplate restTemplate) {
        this.databaseConduit = databaseConduit;
        this.userRepository = userRepository;
        this.restTemplate = restTemplate;
    }

    @KafkaListener(topics = "${general.kafka-topic}")
    public void listen(Transaction transaction) {
        logger.info("received: " + transaction);
        UserRecord sender = userRepository.findById(transaction.getSenderId());
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());
        TransactionRecord transactionRecord = new TransactionRecord(sender, recipient, transaction.getAmount());

        if (sender != null && recipient != null && sender.getBalance() >= transaction.getAmount()) {
            Incentive incentive = restTemplate.postForObject("http://localhost:8080/incentive", transaction, Incentive.class);

            float incentiveAmount;

            if(incentive != null){
                incentiveAmount = incentive.getAmount();
            }else{
                incentiveAmount = 0;
            }

            sender.setBalance(sender.getBalance() - transaction.getAmount());
            recipient.setBalance(recipient.getBalance() + transaction.getAmount() + incentiveAmount);

            transactionRecord.setAmount(transaction.getAmount());
            transactionRecord.setRecipient(recipient);
            transactionRecord.setSender(sender);
            transactionRecord.setIncentive(incentiveAmount);

            databaseConduit.save(sender);
            databaseConduit.save(recipient);
            databaseConduit.save(transactionRecord);

        }
    }
}
