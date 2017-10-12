package com.springkafka.demo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.concurrent.ExecutionException;

@RestController
public class VoteController {

    @Autowired
    Producer producer;

    @Autowired
    Consumer consumer;

    @RequestMapping("/vote")
    public Status vote(@RequestBody Vote vote) throws ExecutionException, InterruptedException {
        producer.send(vote.getName());
        return new Status("ok");
    }


    @RequestMapping(value = "/votes",method = RequestMethod.GET)
    public List<Vote> getVotes(){


        return consumer.getAllVotes();
    }

}
