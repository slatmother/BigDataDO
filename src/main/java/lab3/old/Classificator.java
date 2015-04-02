/*
WITHOUT LIMITING THE FOREGOING, COPYING, REPRODUCTION, REDISTRIBUTION,
REVERSE ENGINEERING, DISASSEMBLY, DECOMPILATION OR MODIFICATION
OF THE SOFTWARE IS EXPRESSLY PROHIBITED, UNLESS SUCH COPYING,
REPRODUCTION, REDISTRIBUTION, REVERSE ENGINEERING, DISASSEMBLY,
DECOMPILATION OR MODIFICATION IS EXPRESSLY PERMITTED BY THE LICENSE
AGREEMENT WITH NETCRACKER.

THIS SOFTWARE IS WARRANTED, IF AT ALL, ONLY AS EXPRESSLY PROVIDED IN
THE TERMS OF THE LICENSE AGREEMENT, EXCEPT AS WARRANTED IN THE
LICENSE AGREEMENT, NETCRACKER HEREBY DISCLAIMS ALL WARRANTIES AND
CONDITIONS WITH REGARD TO THE SOFTWARE, WHETHER EXPRESS, IMPLIED
OR STATUTORY, INCLUDING WITHOUT LIMITATION ALL WARRANTIES AND
CONDITIONS OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE,
TITLE AND NON-INFRINGEMENT.

Copyright (c) 1995-2013 NetCracker Technology Corp.

All Rights Reserved.
*/

package lab3.old;

import java.util.*;

/**
 * Class:
 * Description:
 * <p/>
 * Created by: geal0913
 * Date: 02.04.2015
 */
public final class Classificator {
    private final Map<UserClass, List<String>> classes = new HashMap<UserClass, List<String>>(5);

    public void addDomain(String domain) {
        UserClass uClass = UserClass.resolveDomain(domain);

        if (uClass != null) {
           addClass(uClass, domain);
        }
    }

    public boolean isIT() {
        return resolveClassification(UserClass.IT);
    }

    public boolean isCarDriver() {
        return resolveClassification(UserClass.CAR_DRIVER);
    }

    public boolean isMusicMan() {
        return resolveClassification(UserClass.MUSICMAN);
    }

    public boolean isPolitic() {
        return resolveClassification(UserClass.POLITIC);
    }

    public boolean isTalker() {
        return resolveClassification(UserClass.TALKER);
    }

    private int b2i(boolean b) {
        return b ? 1 : 0;
    }

    public String getVector() {
        return new StringBuilder().
                append(b2i(isIT())).append("\t").
                append(b2i(isCarDriver())).append("\t").
                append(b2i(isTalker())).append("\t").
                append(b2i(isPolitic())).append("\t").
                append(b2i(isMusicMan())).toString();

    }

    private boolean resolveClassification(UserClass userClass) {
        List<String> domains = classes.get(userClass);
        if (domains == null || domains.isEmpty())
            return false;

        Collections.sort(domains);

        int domainsVisits = 0;
        int uniqueDomainsVisit = 0;

        String currentDomain = null;
        boolean result = false;

        for (String domain : domains) {
            if (currentDomain == null) {
                currentDomain = domain;
                uniqueDomainsVisit++;
            } else if (!currentDomain.equals(domain)) {
                currentDomain = domain;
                uniqueDomainsVisit++;
            }

            domainsVisits++;

            if (domainsVisits >= 3 && uniqueDomainsVisit >= 2) {
                result = true;
                break;
            }
        }

        return result;
    }

    private void addClass(UserClass uClass, String domain) {
        List<String> domains = classes.get(uClass);

        if (domains != null) {
            domains.add(domain);
        } else {
            List<String> list = new ArrayList<String>();
            list.add(domain);
            classes.put(uClass, list);
        }
    }

    private enum UserClass {
        IT(
                new String[]{"computerra.ru", "web-ip.ru", "filebase.ws"}
        ),
        CAR_DRIVER(
                new String[]{"cars.ru", "avto-russia.ru", "bmwclub.ru"}
        ),
        TALKER(
                new String[]{"vk.com", "mail.qip.ru", "lk.ssl.mts.ru"}
        ),
        POLITIC(
                new String[]{"novayagazeta.ru", "echo.msk.ru", "inosmi.ru"}
        ),
        MUSICMAN(
                new String[]{"nirvana.fm", "rusradio.ru", "pop-music.ru"}
        );

        private final List<String> domains;

        UserClass(String[] domains) {
            this.domains = Arrays.asList(domains);
        }

        public List<String> getDomains() {
            return domains;
        }

        public static UserClass resolveDomain(String domain) {
            for (UserClass uClass : UserClass.values()) {
                if (uClass.getDomains().contains(domain.toLowerCase())) {
                    return uClass;
                }
            }

            return null;
        }
    }
}

