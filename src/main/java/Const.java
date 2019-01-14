
public class Const {
    /*
     * the zoom-level at which we render our tiles Attention: Code is not generic enough to change this value without
     * further code changes! ;)
     */
    static final int ZOOM = 13;
     
    // used on values
    static final long MAX_TILE_NUMBER  = (long) Math.pow(2, ZOOM) - 1;
}
