import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * J(X,Y) = |X∩Y| / |X∪Y|
 */
public class Shingling {

    private static int k = 9;

    private static String FIRST_GENOME = "Genome_1.txt";
    private static String SECOND_GENOME = "Genome_2.txt";

    private static String GET_ID_BY_FILE_NAME = "SELECT file_id FROM genomes.genome_files f WHERE f.file_name = ?";
    private static String GET_COUNT_BY_FILE_NAME_AND_LENGTH = "SELECT max(id) FROM genomes.sub_gens sg WHERE sg.file_id = ? AND sg.length = ?";
    private static String GET_MAX_FILE_ID = "SELECT max(file_id) FROM genomes.genome_files";
    private static String SAVE_NEW_FILE = "INSERT INTO genomes.genome_files(file_id, file_name) VALUES (?, ?)";
    private static String GET_MAX_SUB_GEN_ID = "SELECT max(id) FROM genomes.sub_gens";
    private static String SAVE_SUB_GEN = "INSERT INTO genomes.sub_gens(id, file_id, sub_gen, length) VALUES (?, ?, ?, ?)";
    private static String CALCULATE_JACCARD_SIMILARITY = "SELECT (SELECT count(*)::decimal(8,2) FROM\n" +
            "  ((SELECT sg.sub_gen FROM genomes.sub_gens sg WHERE sg.file_id = ? AND sg.length = ?)\n" +
            "    INTERSECT\n" +
            "  (SELECT sg.sub_gen FROM genomes.sub_gens sg WHERE sg.file_id = ? AND sg.length = ?)) AS set1)\n" +
            "  /\n" +
            "       (SELECT count(*)::decimal(8,2) FROM\n" +
            "         ((SELECT sg.sub_gen FROM genomes.sub_gens sg WHERE sg.file_id = ? AND sg.length = ?)\n" +
            "         UNION\n" +
            "         (SELECT sg.sub_gen FROM genomes.sub_gens sg WHERE sg.file_id = ? AND sg.length = ?)) AS set2);";

    public static void main(String[] args) throws IOException {

        System.out.println("Jaccard similarity for files '" + FIRST_GENOME + "' and '" +
                SECOND_GENOME + "' = " + calcJaccard(FIRST_GENOME, SECOND_GENOME));
    }

    private static double calcJaccard(String filename1, String filename2) throws IOException {
        uploadFile(filename1);
        uploadFile(filename2);
        double jacValue = 0.0;

        try {
            Connection connection = DatabaseConnection.getConnection();
            PreparedStatement ps;
            ResultSet rs;

            int filename1_id = 0;
            int filename2_id = 0;

            ps = connection.prepareStatement(GET_ID_BY_FILE_NAME);
            ps.setString(1, filename1);
            rs = ps.executeQuery();
            if (rs.next()) {
                filename1_id = rs.getInt(1);
            }

            ps = connection.prepareStatement(GET_ID_BY_FILE_NAME);
            ps.setString(1, filename2);
            rs = ps.executeQuery();
            if (rs.next()) {
                filename2_id = rs.getInt(1);
            }

            ps = connection.prepareStatement(CALCULATE_JACCARD_SIMILARITY);
            ps.setInt(1, filename1_id);
            ps.setInt(2, k);
            ps.setInt(3, filename2_id);
            ps.setInt(4, k);
            ps.setInt(5, filename1_id);
            ps.setInt(6, k);
            ps.setInt(7, filename2_id);
            ps.setInt(8, k);
            rs = ps.executeQuery();
            if (rs.next()) {
                jacValue = rs.getDouble(1);
            }
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return jacValue;
    }

    //read from file
    public static void uploadFile(String filename) throws IOException {
        StringBuilder sb = new StringBuilder();
        List<String> lines = Files.readAllLines(Paths.get(filename), StandardCharsets.UTF_8);
        for (String line : lines) {
            sb.append(line);
        }

        String genome = sb.toString().replaceAll(" ", "").replaceAll("\n", "").replaceAll("\r", "");

        Set<String> subs = new HashSet<>();
        for (int i = 0; i <= genome.length() - k; i++) {
            subs.add(genome.substring(i, i + k));
        }

        uploadGenomeToDb(filename, subs);
    }

    private static void uploadGenomeToDb(String filename, Set<String> subs) {
        try {
            Connection connection = DatabaseConnection.getConnection();

            PreparedStatement ps;
            ResultSet rs;

            int new_file_id;

            //check if file_name is already exists in DB
            ps = connection.prepareStatement(GET_ID_BY_FILE_NAME);
            ps.setString(1, filename);
            rs = ps.executeQuery();
            if (rs.next()) {        //file is already exists in DB
                new_file_id = rs.getInt(1);

                //if this file has already been parsed for entered k
                ps = connection.prepareStatement(GET_COUNT_BY_FILE_NAME_AND_LENGTH);
                ps.setInt(1, new_file_id);
                ps.setInt(2, k);
                rs = ps.executeQuery();
                if (rs.next()) {
                    if (rs.getInt(1) != 0) {
                        System.out.println(rs.getInt(1) + " file '" + filename + "' has already been parsed for k = " + k);
                    } else {
                        saveSubGensIntoDB(subs, new_file_id, connection);
                    }
                }
            } else {
                //get new file_id
                new_file_id = 0;
                ps = connection.prepareStatement(GET_MAX_FILE_ID);
                rs = ps.executeQuery();
                if (rs.next()) {
                    new_file_id = rs.getInt(1) + 1;       //file_id for the next new file
                }

                //save new file name into DB
                ps = connection.prepareStatement(SAVE_NEW_FILE);
                ps.setInt(1, new_file_id);
                ps.setString(2, filename);
                ps.executeUpdate();

                saveSubGensIntoDB(subs, new_file_id, connection);

            }

        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private static void saveSubGensIntoDB(Set<String> subs, int file_id, Connection connection) throws SQLException {

        PreparedStatement ps;
        ResultSet rs;

        //get new_sub_gen_id
        int new_sub_gen_id = 0;
        ps = connection.prepareStatement(GET_MAX_SUB_GEN_ID);
        rs = ps.executeQuery();
        if (rs.next()) {
            new_sub_gen_id = rs.getInt(1);
        }

        //save all subgens into DB
        ps = connection.prepareStatement(SAVE_SUB_GEN);
        for (String sub : subs) {
            ps.setInt(1, ++new_sub_gen_id);
            ps.setInt(2, file_id);
            ps.setString(3, sub);
            ps.setInt(4, k);
            ps.executeUpdate();
        }
    }


}
