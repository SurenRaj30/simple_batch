package com.surenraj.simplebatch;

import org.springframework.batch.item.ItemProcessor;

public class HeartProcessor implements ItemProcessor<Heart, Heart> {

    @Override
    public Heart process(Heart heart) throws Exception {
        return heart;
    }
}
