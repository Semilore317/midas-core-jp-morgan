package com.jpmc.midascore.service;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Balance;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.stereotype.Service;

@Service
public class BalanceService {
    private final UserRepository userRepository;

    public BalanceService(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    public Balance getBalance(long userId) {
        UserRecord user = userRepository.findById(userId);
        if (user != null) {
            return new Balance(user.getBalance());
        }
        return new Balance(0);
    }
}
