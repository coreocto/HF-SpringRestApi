package org.coreocto.dev.hf.springrestapi.controller;

import com.google.common.base.Strings;
import org.apache.log4j.Logger;
import org.coreocto.dev.hf.commonlib.Constants;
import org.coreocto.dev.hf.commonlib.sse.vasst.bean.RelScore;
import org.coreocto.dev.hf.springrestapi.AppConstants;
import org.coreocto.dev.hf.springrestapi.bean.DocInfo;
import org.coreocto.dev.hf.springrestapi.bean.SearchResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

@RestController
public class SearchController {

    private Logger LOGGER = Logger.getLogger(this.getClass());

    @Autowired
    JdbcTemplate jdbcTemplate;

    @RequestMapping(path = "/search")
    public SearchResponse search
            (
                    @RequestParam("st") String st,
                    @RequestParam("q") String[] qValues
            ) {
        SearchResponse response = new SearchResponse();

        String qid = UUID.randomUUID().toString();
        String q = (qValues != null && qValues.length > 0) ? qValues[0] : "";

        int rowCnt = -1;

        if (st.equalsIgnoreCase(Constants.SSE_TYPE_SUISE + "")) {
            rowCnt = jdbcTemplate.update("insert into tquery_statistics (cqueryid,cstarttime,cdata) values (?,?,?)", new PreparedStatementSetter() {
                @Override
                public void setValues(PreparedStatement pStmnt) throws SQLException {
                    pStmnt.setString(1, qid);
                    pStmnt.setLong(2, System.currentTimeMillis());
                    pStmnt.setString(3, q);
                }
            });

            if (rowCnt != 1) {
                String msg = "error when inserting tquery_statistics record";
                response.setMessage(msg);
                response.setStatus(AppConstants.STATUS_ERR);
                LOGGER.error(msg);
            } else {
                SqlRowSet result = jdbcTemplate.queryForRowSet("select cdocid, cft, cfeiv from tdocuments t where exists(select 1 from tdocument_indexes t2 where t.cdocid = t2.cdocid and H(?,R(corder))||R(corder) = ctoken)", new String[]{q});
                //queryForRowSet does not support PreparedStatementSetter, don't use it here

                while (result.next()) {
                    String docId = result.getString(1);
                    Integer type = result.getInt(2);
                    String feiv = result.getString(3);

                    DocInfo docInfo = new DocInfo();
                    docInfo.setName(docId);
                    docInfo.setType(type);
                    docInfo.setFeiv(feiv);

                    response.getFiles().add(docInfo);
                }

                int fileCnt = response.getFiles().size();

                response.setCount(fileCnt);

                rowCnt = jdbcTemplate.update("update tquery_statistics set cendtime = ?, cmatchedcnt = ? where cqueryid = ?", new PreparedStatementSetter() {
                    @Override
                    public void setValues(PreparedStatement pStmnt) throws SQLException {
                        pStmnt.setLong(1, System.currentTimeMillis());
                        pStmnt.setInt(2, fileCnt);
                        pStmnt.setString(3, qid);
                    }
                });

                if (rowCnt != 1) {
                    String msg = "error when updating tquery_statistics record";
                    response.setMessage(msg);
                    response.setStatus(AppConstants.STATUS_ERR);
                    LOGGER.error(msg);
                    response.getFiles().clear();
                    response.setCount(0);
                }
            }
        } else if (st.equalsIgnoreCase(Constants.SSE_TYPE_VASST + "")) {
            //compute the document score
            //each matched document will first compute the TF-IDF (the term freq. will be normalized by max occurrence
            //and each word inside query vector will be computed and normalized with max occurrence also
            //then we will compute the the score using the inner product of two vectors and sort in descending order

            final int MAX_RESULT = 10;

            int numOfQueryTerms = qValues.length;

            String placeHolders = Strings.repeat("?,", numOfQueryTerms).substring(0, 2 * numOfQueryTerms - 1);

            // find the term freq. inside the query vector
            Map<String, Integer> queryTermOccur = new HashMap<>();
            for (String queryTerm : qValues) {
                if (queryTermOccur.containsKey(queryTerm)) {
                    int occur = queryTermOccur.get(queryTerm);
                    queryTermOccur.put(queryTerm, occur + 1);
                } else {
                    queryTermOccur.put(queryTerm, 1);
                }
            }

            // find the max term freq. inside the query vector
            int maxQryTerms = 0;
            for (Integer i : queryTermOccur.values()) {
                maxQryTerms = Math.max(i, maxQryTerms);
            }

            List<RelScore> relScores = new ArrayList<>();

            List<String> matchedDocIds = new ArrayList<>();

            Map<String, DocInfo> docTypeLookup = new HashMap<>();

            //queryForRowSet does not support PreparedStatementSetter, don't use it here
            SqlRowSet rs = jdbcTemplate.queryForRowSet("select cdocid, cft, cfeiv from tdocuments d where exists(select 1 from tdoc_term_freq dtf where d.cdocid = dtf.cdocid and dtf.cword in (" + placeHolders + "))", qValues);

            while (rs.next()) {
                String docId = rs.getString(1);
                matchedDocIds.add(docId);
                int type = rs.getInt(2);
                String feiv = rs.getString(3);

                DocInfo docInfo = new DocInfo();
                docInfo.setName(docId);
                docInfo.setType(type);
                docInfo.setFeiv(feiv);

                docTypeLookup.put(docId, docInfo);
            }

            int matchedDocCnt = matchedDocIds.size();

            int docCnt = 0;

            docCnt = (Integer) jdbcTemplate.queryForObject("select count(*) from tdocuments d where exists(select 1 from tdoc_term_freq dtf where d.cdocid = dtf.cdocid)", Integer.class);

            Map<String, RelScore> docScoreMap = new HashMap<>();

            //calculate tf-idf
            for (int i = 0; i < matchedDocCnt; i++) {
                String curDocId = matchedDocIds.get(i);

                //construct the param array
                Object[] param = new Object[qValues.length + 1];
                param[0] = curDocId;
                for (int j = qValues.length - 1; j >= 0; j--) {
                    param[j + 1] = qValues[j];
                }
                //end

                SqlRowSet tmpRs = jdbcTemplate.queryForRowSet("select " +
                        "dtf.ccount, (select max(ccount) from tdoc_term_freq dtf2 where dtf.cword=dtf2.cword) max_ccount, " +
                        "cword " +
                        "from tdoc_term_freq dtf where cdocid = ? and cword in (" + placeHolders + ")", param);

                while (tmpRs.next()) {
                    int tf = tmpRs.getInt(1);  //term freq.
                    int mtf = tmpRs.getInt(2); //max term freq.
                    String word = tmpRs.getString(3); //the encrypted keyword, not necessary
                    double ntf = tf * 1.0 / mtf; //normalized term freq.
                    double tfidf = ntf * Math.log(docCnt * 1.0 / matchedDocCnt);

                    RelScore relScore = null;

                    if (docScoreMap.containsKey(curDocId)) {
                        relScore = docScoreMap.get(curDocId);
                    } else {
                        relScore = new RelScore();
                        relScore.setDocId(curDocId);
                        docScoreMap.put(curDocId, relScore);
                    }
                    double oldScore = relScore.getScore();
                    Integer qryTermFreq = queryTermOccur.get(word);
                    if (qryTermFreq == null) {
                        qryTermFreq = 0;
                    }
                    double queryTermScore = qryTermFreq * 1.0 / maxQryTerms;
                    relScore.setScore(oldScore + (tfidf * queryTermScore));
                }
                //end
            }

            relScores.addAll(docScoreMap.values());

            Collections.sort(relScores, new Comparator<RelScore>() {
                public int compare(RelScore o1, RelScore o2) {
                    return (new Double(o2.getScore())).compareTo(o1.getScore());
                }
            });

            int minResult = Math.min(relScores.size(), MAX_RESULT);

            for (int x = 0; x < minResult; x++) {
                String docId = relScores.get(x).getDocId();
                DocInfo tmp = docTypeLookup.get(docId);
                response.getFiles().add(tmp);
            }

            response.setCount(response.getFiles().size());
            response.setTotalCount(relScores.size());

        } else if (st.equalsIgnoreCase(Constants.SSE_TYPE_CHLH + "")) {
            SqlRowSet rs = jdbcTemplate.queryForRowSet("select cdocid, cft, cfeiv from tdocuments d where exists(select 1 from tchlh d2 where chlh_search(d2.cbf, ?) = ? and d.cdocid = d2.cdocid)", q, 1);

            while (rs.next()) {
                DocInfo tmp = new DocInfo();
                tmp.setName(rs.getString(1));
                tmp.setType(rs.getInt(2));
                tmp.setFeiv(rs.getString(3));
                response.getFiles().add(tmp);
            }

            int totalCnt = response.getFiles().size();
            response.setCount(totalCnt);
            response.setTotalCount(totalCnt);
        }

        return response;
    }

}
