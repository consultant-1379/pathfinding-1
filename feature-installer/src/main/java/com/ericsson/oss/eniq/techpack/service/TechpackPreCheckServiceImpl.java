package com.ericsson.oss.eniq.techpack.service;

import com.ericsson.oss.eniq.techpack.globalexception.TechPackExceptions;
import com.ericsson.oss.eniq.techpack.model.Versioning;
import com.ericsson.oss.eniq.techpack.service.interfaces.TechpackPreCheckService;
import com.ericsson.oss.eniq.techpack.service.interfaces.VersioningService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Service;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Service
@Qualifier("techpackPreCheckServiceImpl")
public class TechpackPreCheckServiceImpl implements TechpackPreCheckService {

    private static final String INSTALLATION_OF_TECH_PACK_FAILED = "Installation of tech pack failed.";
    private static final String UNSERSCORE = "_";
    private static final String RSTATE_HAS_INVALID_FORMAT = "Rstate {} has invalid format.";
    private static final String FIND_B = "_b";
    private Logger logger = LoggerFactory.getLogger(TechpackPreCheckServiceImpl.class);

    @Autowired
    private ResourceLoader resourceLoader;

    @Lazy
    @Autowired
    private VersioningService versioningService;

    @Override
    public Map<String, String> readVersionPropertyFile() throws IOException {
        logger.trace("Inside readVersionPropertyFile() method");
        Properties props = new Properties();
        Resource resource = resourceLoader.getResource("classpath:version.properties");

        try (InputStream inputStream = new FileInputStream(resource.getFile())) {
            logger.info("Reading file: {}", resource.getFilename());
            props.load(inputStream);
            String techPackName = props.getProperty("tech_pack.name");
            if (techPackName == null || techPackName.length() <= 0) {
                throw new TechPackExceptions(
                        "Required entry tech_pack.name was not found from version.properties file. Please check tech pack's version.properties file. Tech pack installation aborted.");
            }

            String techPackVersion = props.getProperty("tech_pack.version");
            if (techPackVersion == null || techPackVersion.length() <= 0)
                throw new TechPackExceptions(
                        "Required entry tech_pack.version was not found from version.properties. Please check tech pack's version.properties file. Tech pack installation aborted.");

            checkTpMetadataVersion(props);

            String buildNumber = props.getProperty("build.number");
            if (buildNumber == null || buildNumber.length() <= 0) {
                throw new TechPackExceptions(
                        "Required entry build.number was not found from version.properties. Please check tech pack's version.properties file. Tech pack installation aborted.");
            }

            return props.entrySet().stream().filter(e -> e.getKey().toString().startsWith("required_tech_packs"))
                    .collect(Collectors.toMap(e -> ((String) e.getKey()).substring(((String) e.getKey()).indexOf(".") + 1),
                            e -> e.getValue().toString().indexOf(FIND_B, 1) >= 1 ? e.getValue().toString().substring(0, e.getValue().toString().indexOf(FIND_B)) : e.getValue().toString()));
        } catch (IOException e) {
            throw new IOException("Could not read file " + resource.getFilename() + ". Please check that the file "
                    + resource.getFilename() + " exists and it can be read.");
        }

    }

    private void checkTpMetadataVersion(Properties props) {
        try {
            Integer tpMetadataVers = Integer.parseInt(props.getProperty("tech_pack.metadata_version"));
            logger.info("This tech pack uses metadata version {}", tpMetadataVers);
        } catch (final Exception e) {
            logger.info("Techpack metadata version not propely set. Assuming version 1.");
        }
    }


    @Override
    public void checkRequiredTechPackInstallations(Map<String, String> requiredTechPackInstallations) {
        logger.trace("Inside checkDependencies() method");
        for (Map.Entry<String, String> entry : requiredTechPackInstallations.entrySet()) {
            String requiredTechPackVersion = entry.getValue();
            String requiredTechPackName = entry.getKey();
            String requiredProdNum = "";

            if (requiredTechPackVersion.contains(":")) {
                // The COA-number is in the required techpacks version. Parse it out from there.
                final String[] splittedTPVersion = requiredTechPackVersion.split(":");
                requiredProdNum = splittedTPVersion[0];
                requiredTechPackVersion = splittedTPVersion[1];
            }

            final Versioning targetVersioning = versioningService.getTargetVersioning(requiredTechPackName);

            if (targetVersioning == null) {
                logger.info("Required tech pack {} of at least version {} is not found. Please install required tech pack before installation can continue.", requiredTechPackName, requiredTechPackVersion);
                throw new TechPackExceptions("Installation of tech pack failed to missing dependency package.");
            } else {
                String techPackVersion = targetVersioning.getTechpack_version();
                final String techPackProdNum = targetVersioning.getProduct_number();

                if (requiredProdNum.equalsIgnoreCase("") && techPackProdNum.contains("/")) {
                    // It's ok to install this TP. Not really need to compare RStates in this situation.
                    logger.info("Newer version (according to COA-number) {} of required tech pack {} is already installed. Installation can continue.", requiredTechPackVersion, requiredTechPackName);
                } else if (!requiredProdNum.equalsIgnoreCase("")) { // NOPMD
                    // There is a product number in the version information.
                    final Integer result = compareProductNumbers(requiredProdNum, techPackProdNum);
                    if (result == 1) {
                        logger.info("Required tech pack {} has older version (according to COA-number) {} installed. Please update it to at least to version {} before installation can continue.", requiredTechPackName, techPackProdNum, requiredProdNum);
                        throw new TechPackExceptions(INSTALLATION_OF_TECH_PACK_FAILED);
                    } else if (result == -1) {
                        logger.info("Newer version (according to COA-number) {} of required tech pack {} is already installed. Installation can continue.", requiredTechPackVersion, requiredTechPackName);
                    } else {
                        // Product numbers are the same. Start comparing the RStates. Drop off the _b123 if it exists in the techpack's version.
                        compareRStateBasedOnVersionOld_New(requiredTechPackVersion, requiredTechPackName, techPackVersion);
                    }

                } else {
                    // Product number is empty so compare the RStates then...
                    // Drop off the _b123 if it exists in the techpack's version.
                    compareRStateBasedOnVersionOld_New(requiredTechPackVersion, requiredTechPackName, techPackVersion);
                }
            }
        }
    }

    private void compareRStateBasedOnVersionOld_New(String requiredTechPackVersion, String requiredTechPackName, String techPackVersion) {
        if (techPackVersion.indexOf(UNSERSCORE, 1) >= 1) {
            techPackVersion = techPackVersion.substring(0, techPackVersion.lastIndexOf(UNSERSCORE));
        }

        final Integer rstateCompResult = compareRstates(techPackVersion, requiredTechPackVersion);

        if ((rstateCompResult == 0) || (rstateCompResult == 1)) {
            // The required Rstate is the same or newer than required.
            logger.info("Newer or the same version {} of required tech pack {} is already installed. Installation can continue.", requiredTechPackVersion, requiredTechPackName);
        } else {
            logger.info("Required tech pack {} has older version {} installed. Please update it to at least to version {} before installation can continue.", requiredTechPackName, techPackVersion, requiredTechPackVersion);
            throw new TechPackExceptions(INSTALLATION_OF_TECH_PACK_FAILED);
        }
    }

    /**
     * This function compares two different product numbers and returns an Integer telling the comparison result.
     * Returns 0 if the product numbers are equal, returns 1 if the first product number is bigger, returns -1 if the
     * second product number is bigger and returns 2 if the product numbers are in incorrect format. Example product
     * numbers could be: COA 123 456, COA 123 558/1, COA 121 981/3 etc.
     *
     * @param oldProdNum first product number (old one).
     * @param newProdNum second product number (new one).
     * @return Returns the comparison result.
     */
    private Integer compareProductNumbers(final String oldProdNum, final String newProdNum) {
        // If the old one contains "/" character and the new product number doesn't,
        // then upgrade is never done.
        if (oldProdNum.contains("/") && !newProdNum.contains("/"))
            return 1;
        // If the old one does not contain "/" character and the new product number does, upgrade is done every time.
        if (!oldProdNum.contains("/") && newProdNum.contains("/"))
            return -1;
        // If both old and new product numbers include "/" character, then
        // compare the number after the "/" character. Bigger number is newer and should be updated.
        if (oldProdNum.contains("/") && newProdNum.contains("/")) {
            final Integer oldProdNumExtension = Integer.valueOf(oldProdNum.substring((oldProdNum.lastIndexOf("/") + 1), oldProdNum.length())); // Example : 1
            final Integer newProdNumExtension = Integer.valueOf(newProdNum.substring((newProdNum.lastIndexOf("/") + 1), newProdNum.length())); // Example : 2
            return Integer.compare(oldProdNumExtension, newProdNumExtension);
        }

        if (oldProdNum.equalsIgnoreCase(newProdNum)) {
            return 0;
        }
        return 2;
    }

    /**
     * This function compares two RStates. The RState format is "R19C". Returns 0 if the RStates are equal, returns 1 if
     * the firstRstate is bigger, returns 2 if the secondRstate is bigger and returns -1 if the Rstates are in incorrect
     * format.
     *
     * @param firstRstate  First RSTATE
     * @param secondRstate Second RSTATE
     * @return Integer 0 --> RSTATE are equal 1 --> First RSTATE > Second RSTATE 2 --> Second RSTATE > First RSTATE
     */
    private Integer compareRstates(final String firstRstate, final String secondRstate) {
        // Use regexp to get the number value of RState.
        final Pattern pattern = Pattern.compile("\\d+");
        final Matcher matcher = pattern.matcher(firstRstate);

        if (!matcher.find()) {
            logger.info(RSTATE_HAS_INVALID_FORMAT, firstRstate);
            return -1;
        }

        final String firstRstateNum = matcher.group(0);
        final Matcher matcher2 = pattern.matcher(secondRstate);

        final Pattern pattern2 = Pattern.compile(".$");
        final Matcher matcher3 = pattern2.matcher(firstRstate);

        if (!matcher3.find()) {
            logger.info(RSTATE_HAS_INVALID_FORMAT, firstRstate);
            return -1;
        }

        if (!matcher2.find()) {
            logger.info(RSTATE_HAS_INVALID_FORMAT, secondRstate);
            return -1;
        }
        final String firstRstateLastChar = matcher3.group(0);
        final Matcher matcher4 = pattern2.matcher(secondRstate);

        if (!matcher4.find()) {
            logger.info(RSTATE_HAS_INVALID_FORMAT, secondRstate);
            return -1;
        }
        final String secondRstateLastChar = matcher4.group(0);
        final String secondRstateNum = matcher2.group(0);

        if (Integer.parseInt(firstRstateNum) == Integer.parseInt(secondRstateNum)) {
            // The RState numbers are equal. Check the string after RState number which is bigger.
            if (firstRstateLastChar.compareTo(secondRstateLastChar) == 0) {
                return 0;
            } else if (firstRstateLastChar.compareTo(secondRstateLastChar) > 0) {
                return 1;
            } else {
                return 2;
            }
        } else {
            // Let the Rstate number decide which is bigger.
            if (Integer.parseInt(firstRstateNum) > Integer.parseInt(secondRstateNum)) {
                return 1;
            } else {
                return 2;
            }
        }
    }

}