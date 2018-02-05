package org.coreocto.dev.hf.springrestapi.controller;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.log4j.Logger;
import org.coreocto.dev.hf.springrestapi.AppConstants;
import org.coreocto.dev.hf.springrestapi.bean.BaseResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.sql.PreparedStatement;
import java.sql.SQLException;

@RestController
public class StatisticsController {

    private Logger LOGGER = Logger.getLogger(this.getClass());

    @Autowired
    JdbcTemplate jdbcTemplate;

    @RequestMapping(path = "/stat")
    public BaseResponse stat
            (
                    @RequestParam("data") String q,
                    @RequestParam("type") String type
            ) {
        BaseResponse response = new BaseResponse();
        response.setMessage(AppConstants.MSG_OK);
        response.setStatus(AppConstants.STATUS_OK);

        JsonObject jsonObj = null;

        Gson gson = new Gson();

        try {
            jsonObj = gson.fromJson(q, JsonObject.class);
        } catch (Exception ex) {
            String msg = "error when parsing json into object";
            LOGGER.error(msg, ex);
            response.setStatus(AppConstants.STATUS_ERR);
            response.setMessage(msg);
        }

        if (response.getStatus() < AppConstants.STATUS_ERR) {
            final JsonObject jsonObjRef = jsonObj;

            int affectRows = -1;

            affectRows = jdbcTemplate.update("INSERT INTO public.tstatistics(cdocid, cstarttime, cendtime, cwordcnt, cfilesize, ctype) VALUES (?, ?, ?, ?, ?, ?)", new PreparedStatementSetter() {

                @Override
                public void setValues(PreparedStatement pStmnt) throws SQLException {
                    pStmnt.setString(1, jsonObjRef.get("name").getAsString());
                    pStmnt.setLong(2, jsonObjRef.get("startTime").getAsLong());
                    pStmnt.setLong(3, jsonObjRef.get("endTime").getAsLong());
                    if (jsonObjRef.get("wordCount") != null) {
                        pStmnt.setLong(4, jsonObjRef.get("wordCount").getAsLong());
                    } else {
                        pStmnt.setNull(4, java.sql.Types.BIGINT);
                    }
                    if (jsonObjRef.get("fileSize") != null) {
                        pStmnt.setLong(5, jsonObjRef.get("fileSize").getAsLong());
                    } else {
                        pStmnt.setNull(5, java.sql.Types.BIGINT);
                    }
                    pStmnt.setString(6, type);
                }
            });

            if (affectRows == -1) {
                String msg = "error when inserting tstatistics record";
                LOGGER.error(msg);
                response.setMessage(msg);
                response.setStatus(AppConstants.STATUS_ERR);
            }
        }

        return response;
    }
}
