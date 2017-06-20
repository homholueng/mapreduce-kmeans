package trans.model;

/**
 * Created by HL on 20/06/2017.
 */
public class News {
    private String id;
    private int typeId;
    private String title;
    private String url;
    private String mainType;
    private String subType;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getTypeId() {
        return typeId;
    }

    public void setTypeId(int typeId) {
        this.typeId = typeId;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getMainType() {
        return mainType;
    }

    public void setMainType(String mainType) {
        this.mainType = mainType;
    }

    public String getSubType() {
        return subType;
    }

    public void setSubType(String subType) {
        this.subType = subType;
    }

    public static void main(String[] args) {
        System.out.println("fffb759a-ed3d-4f06-8646-431d69955f18".length());
    }
}
