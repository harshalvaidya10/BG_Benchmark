/**
 * @description
 * @date 2025/2/14 8:49
 * @version 1.0
 */

package janusgraph;

import edu.usc.bg.base.ByteIterator;
import edu.usc.bg.base.DBException;
import edu.usc.bg.base.StringByteIterator;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;

public class janusGraphTest {
    public static void main(String[] args) throws DBException {
        JanusGraphClient janusGraphClient = new JanusGraphClient();
        janusGraphClient.init();
        janusGraphClient.insertEntity("users", String.valueOf(0), new HashMap<>(), false);
        janusGraphClient.insertEntity("users", String.valueOf(1), new HashMap<>(), false);
        janusGraphClient.CreateFriendship(1, 2);
        janusGraphClient.thawFriendship(1, 2);
//        HashMap<String, ByteIterator> values = new HashMap<>();
//        HashMap<String, ByteIterator> values2 = new HashMap<>();
//
//        // 添加属性
//        values.put("username", new StringByteIterator("Bob"));
//        values.put("pw", new StringByteIterator("password123"));
//        values.put("firstname", new StringByteIterator("Bob"));
//        values.put("lastname", new StringByteIterator("Doe"));
//        values.put("gender", new StringByteIterator("male"));
//        values.put("dob", new StringByteIterator("1990-01-01"));
//        values.put("jdate", new StringByteIterator("2022-01-01"));
//        values.put("ldate", new StringByteIterator("2025-01-01"));
//        values.put("address", new StringByteIterator("123 Main St, Los Angeles, CA"));
//        values.put("email", new StringByteIterator("john.doe@example.com"));
//        values.put("tel", new StringByteIterator("123-456-7890"));
//        values.put("imageid", new StringByteIterator("img1"));
//        values.put("thumbnailid", new StringByteIterator("thumb1"));
//
//        // 添加属性
//        values.put("username", new StringByteIterator("mary"));
//        values.put("pw", new StringByteIterator("password1234"));
//        values.put("firstname", new StringByteIterator("Mary"));
//        values.put("lastname", new StringByteIterator("Doe"));
//        values.put("gender", new StringByteIterator("female"));
//        values.put("dob", new StringByteIterator("1990-02-01"));
//        values.put("jdate", new StringByteIterator("2022-02-01"));
//        values.put("ldate", new StringByteIterator("2025-02-01"));
//        values.put("address", new StringByteIterator("1234 Main St, Los Angeles, CA"));
//        values.put("email", new StringByteIterator("john.doe@example.com"));
//        values.put("tel", new StringByteIterator("123-456-7890"));
//        values.put("imageid", new StringByteIterator("img1"));
//        values.put("thumbnailid", new StringByteIterator("thumb1"));
//        janusGraphClient.insertEntity("users", "0", values, false);
//        janusGraphClient.insertEntity("users", "1", values2, false);
//        janusGraphClient.viewProfile(0, 0, new HashMap<>(), false, false);
//        janusGraphClient.viewProfile(0, 1, new HashMap<>(), false, false);
//        janusGraphClient.inviteFriend(0, 1);
//        janusGraphClient.viewFriendReq(1, new Vector<>(), false, false);
//        janusGraphClient.queryPendingFriendshipIds(1, new Vector<>());
//        janusGraphClient.acceptFriend(1, 0);
//        janusGraphClient.queryConfirmedFriendshipIds(1, new Vector<>());
//        janusGraphClient.listFriends(0, 0, new HashSet<>(), new Vector<>(), false, false);
//        janusGraphClient.thawFriendship(0, 1);
//        janusGraphClient.CreateFriendship(0, 1);

    }
}
