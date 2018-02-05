package org.coreocto.dev.hf.springrestapi.controller;

import org.coreocto.dev.hf.springrestapi.bean.BaseResponse;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class PingController {

    @RequestMapping(path = "/ping")
    public BaseResponse ping(){
        BaseResponse response = new BaseResponse();
        response.setMessage("ok");
        response.setStatus(200);
        return response;
    }
}
