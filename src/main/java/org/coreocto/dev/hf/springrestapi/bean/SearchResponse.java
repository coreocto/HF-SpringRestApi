package org.coreocto.dev.hf.springrestapi.bean;

import java.util.ArrayList;
import java.util.List;

public class SearchResponse extends BaseResponse {
    private int count = 0;

    public int getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(int totalCount) {
        this.totalCount = totalCount;
    }

    private int totalCount = 0;
    private List<DocInfo> files = new ArrayList<>();

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public List<DocInfo> getFiles() {
        return files;
    }

    public void setFiles(List<DocInfo> files) {
        this.files = files;
    }

}
