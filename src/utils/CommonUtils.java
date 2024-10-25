package utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.esri.core.geometry.Point;
import common.GpsPoint;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 通用工具方法。非必要时不用Scala,能用Java就用Java，毕竟Scala能兼容Java，但是Java里调用不了Scala的方法
 */
public class CommonUtils {

    public final static String DATA_FACTORY_PATH = "C:\\Users\\syc\\Desktop\\DataFactory\\";


    public static InputStream getResourceFilePath(String fileName) throws FileNotFoundException {
        return CommonUtils.class.getClassLoader().getResourceAsStream(fileName);
    }

    /**
     * 根据路径 不存在则创建文件
     * @param filePath
     * @return
     */
    public static File createFile(String filePath) {
        File file = new File(filePath);
        if (!file.exists()) {
            //getParentFile() 获取上级目录（包含文件名时无法直接创建目录的）
            if (!file.getParentFile().exists()) {
                //创建上级目录
                file.getParentFile().mkdirs();
            }
            try {
                //在上级目录里创建文件
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return file;
    }


    /**
     * 不存在则创建文件及其路径 存在则追加写
     */
    public static FileWriter getAppendWriter(String filePath) throws IOException {
        CommonUtils.createFile(filePath);
        return new FileWriter(filePath, true);
    }

    /**
     * 不存在则创建文件及其路径 存在则覆盖写
     */
    public static FileWriter getOverWriter(String filePath) throws IOException {
        CommonUtils.createFile(filePath);
        return new FileWriter(filePath, false);
    }

    /**
     *  java 获取 获取某年某月 所有日期（yyyy-mm-dd格式字符串）
     * @param year
     * @param month
     * @return
     */
    public static List<String> getMonthFullDay(int year , int month){
        SimpleDateFormat dateFormatYYYYMMDD = new SimpleDateFormat("yyyy-MM-dd");
        List<String> fullDayList = new ArrayList<String>(32);
        // 获得当前日期对象
        Calendar cal = Calendar.getInstance();
        cal.clear();// 清除信息
        cal.set(Calendar.YEAR, year);
        // 1月从0开始
        cal.set(Calendar.MONTH, month-1 );
        // 当月1号
        cal.set(Calendar.DAY_OF_MONTH,1);
        int count = cal.getActualMaximum(Calendar.DAY_OF_MONTH);
        for (int j = 1; j <= count ; j++) {
            fullDayList.add(dateFormatYYYYMMDD.format(cal.getTime()));
            cal.add(Calendar.DAY_OF_MONTH,1);
        }
        return fullDayList;
    }

    /**
     * 获取[startDate, endDate]区间内的所有日期（yyyy-mm-dd格式字符串）
     */
    public static List<String> getDays(String startDate, String endDate) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        List<String> dateList = new ArrayList<String>();
        try {
            Date dateOne = sdf.parse(startDate);
            Date dateTwo = sdf.parse(endDate);
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(dateOne);
            dateList.add(startDate);
            while (dateTwo.after(calendar.getTime())) {
                calendar.add(Calendar.DAY_OF_MONTH, 1);
                dateList.add(sdf.format(calendar.getTime()));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return dateList;
    }

    public static void extractData() throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("testData/GetMaxErrorRoad/maxErrorRoad.txt"), "utf-8"));

        FileWriter fw = new FileWriter("testData/roads.txt");
        BufferedWriter bw = new BufferedWriter(fw);

        String line = reader.readLine();
        while (line != null) {
            bw.write("'" + line.substring(1, line.length()-1).split(",")[0] + "',");
            line = reader.readLine();
        }
        bw.close();
    }

    /**
     * 将字符串内容写到一个新文件（不可覆盖）
     * @param filePath
     * @param content
     */
    public static void printlnToNewFile(String filePath, String content) {
        try {
            File file =new File(filePath);
            if(file.exists()){
                System.out.println("创建失败，" + filePath+ "文件已存在！！！！");
                return;
            }
            FileWriter fw = new FileWriter(filePath);
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write(content);
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 将UTC格式的时间字符串转换为对应的时间戳
     * @param utcTime
     * @return
     * @throws ParseException
     */
    @Deprecated
    public static long formatUtcTimeToTS(String utcTime) {
        String pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
        try {
            return simpleDateFormat.parse(utcTime).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return 0;
    }

    /**
     * 由于对于每个数据都要调用utcTimeToTS()方法，若每次都创建一个SimpleDateFormat对象则会造成较大的内存浪费
     * 而由于SimpleDateFormat中使用的Calendar对象中存放日期数据的变量是线程不安全的，故不能直接设置一个静态变量SimpleDateFormat对象让所有线程共享
     * 故使用ThreadLocal让每个线程持有一份SimpleDateFormat对象，线程内共享，这样在线程内就不用每调用一次utcTimeToTS()方法就创建一个SimpleDateFormat对象了
     * 一个分区对应一个线程，ThreadLocal不回收影响不大，进程结束就没了，要改进的话就要使用mapPartitions算子，每个分区内创建一个SimpleDateFormat对象，然后对分区内数据进行迭代
     */
    public static ThreadLocal<SimpleDateFormat> safeUtcSdf = new ThreadLocal<SimpleDateFormat> () {
        @Override
        protected SimpleDateFormat initialValue() {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
//            simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));//设置时区为世界标准时UTC

            //咱们的数据中的时间应该是东八区时间
            simpleDateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));//设置时区为东八区时间
            return simpleDateFormat;
        }
    };
    public static long utcTimeToTS(String utcTime) throws Exception {
        return safeUtcSdf.get().parse(utcTime).getTime();
    }

    /**
     * 将指定pattern的时间字符串time转换为时间戳
     */
    public static long timeToTS(String time, String pattern) throws Exception {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
        return simpleDateFormat.parse(time).getTime();
    }

    /**
     * 将时间戳转换为时间
     */
    public static String stampToTime(long timestamp, String pattern) throws Exception{
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
        //将时间戳转换为时间
        Date date = new Date(timestamp);
        return simpleDateFormat.format(date);
    }

    /**
     * 将时间戳转换为时间 默认为本地时间 即东八区时间 即北京/上海时间
     */
    public static String stampToTime(long timestamp) throws Exception{
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        //将时间戳转换为时间
        Date date = new Date(timestamp);
        //将时间调整为yyyy-MM-dd_HH:mm:ss时间样式
        return simpleDateFormat.format(date);
    }

    public static int getTimeSlot(long time) { // 解析时间戳，提取出时、分，计算所属时间段编号
        try {
            String str = stampToTime(time, "HH:mm");
            int hour = Integer.parseInt(str.split(":")[0]);
            int minute = Integer.parseInt(str.split(":")[1]);
            // 计算时间差，转换为分钟数
            int minutesSinceDayStart = hour * 60 + minute;
            // 计算时间段编号
            return minutesSinceDayStart / 5;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return -1;
    }

    /**
     * Calculate distance between two points in latitude and longitude taking
     * into account height difference. If you are not interested in height
     * difference pass 0.0. Uses Haversine method as its base.
     *
     * lat1, lon1 Start point lat2, lon2 End point el1 Start altitude in meters
     * el2 End altitude in meters
     * @returns 输出单位：m
     * from : https://stackoverflow.com/questions/3694380/calculating-distance-between-two-points-using-latitude-longitude
     */
    public static double gpsDistance(double lon1, double lat1, double lon2, double lat2) {

        final double R = 6371.137; // Radius of the earth

        double latDistance = Math.toRadians(lat2 - lat1);
        double lonDistance = Math.toRadians(lon2 - lon1);
        double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        double distance = R * c * 1000; // convert to meters


        distance = Math.pow(distance, 2);

        return Math.sqrt(distance);
    }

    public static double getGpsDistance(GpsPoint p1, GpsPoint p2) {
        return gpsDistance(p1.getLongitude(), p1.getLatitude(), p2.getLongitude(), p2.getLatitude());
    }

    /**
     * 将JSON字符串格式化输出
     */
    public static String jsonFormat(String jsonStr) {
        JSONObject jsonObject = JSONObject.parseObject(jsonStr);
        return JSON.toJSONString(jsonObject, SerializerFeature.PrettyFormat,
                SerializerFeature.WriteMapNullValue,
                SerializerFeature.WriteDateUseDateFormat);
    }

    public static double toRadians(double degrees) {
        return degrees * (Math.PI / 180);
    }

    /**
     * 计算两个坐标点连线的方位角
     */
    public static double calculateAzimuth(Point p1, Point p2) {
        double longitude1 = toRadians(p1.getX());
        double longitude2 = toRadians(p2.getX());
        double latitude1 = toRadians(p1.getY());
        double latitude2 = toRadians(p2.getY());

        double longDiff = longitude2 - longitude1;
        double y = Math.sin(longDiff) * Math.cos(latitude2);
        double x = Math.cos(latitude1) * Math.sin(latitude2) - Math.sin(latitude1) * Math.cos(latitude2) * Math.cos(longDiff);

        return (Math.toDegrees(Math.atan2(y, x)) + 360) % 360;
    }

    /**
     * 计算方位角差异 范围[0, 180]
     */
    public static double azimuthDifference(double azimuth1, double azimuth2) {
        double difference = Math.abs(azimuth1 - azimuth2);
        if (difference > 180) {
            difference = 360 - difference;
        }
        return difference;
    }

    public static void main(String[] args) {
        Point p1 = new Point(114.113506, 22.599872);
        Point p2 = new Point(114.1142, 22.600706);
        Point c1 = new Point(114.11354, 22.599849);
        Point c2 = new Point(114.114217, 22.600694);
        double v1 = calculateAzimuth(p1, p2);
        double v2 = calculateAzimuth(c1, c2);
        System.out.println(v1);
        System.out.println(v2);
        System.out.println(azimuthDifference(v1, v2));
    }

    public static void processSequence(int[] sequence) {
        for (int i = 0; i < sequence.length; i++) {
            if (sequence[i] == -1) {
                int left = findLeftNearest(sequence, i);
                int right = findRightNearest(sequence, i);
                int newValue;
                if (left != -1 && right != -1) {
                    newValue = (left + right) / 2;
                } else {
                    newValue = left != -1 ? left : right;
                }
                sequence[i] = newValue;
            }
        }
    }

    private static int findLeftNearest(int[] sequence, int index) {
        for (int i = index - 1; i >= 0; i--) {
            if (sequence[i] != -1) {
                return sequence[i];
            }
        }
        return -1;
    }

    private static int findRightNearest(int[] sequence, int index) {
        for (int i = index + 1; i < sequence.length; i++) {
            if (sequence[i] != -1) {
                return sequence[i];
            }
        }
        return -1;
    }

}
