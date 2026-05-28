import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.util.Collection;
import java.util.Set;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/5/28
 */
public class TestAll {


    @org.junit.Test
    public void findAll() {

        Set<String> reservedFile = Set.of("com.qlangtech.tis.plugin.ontology.Ontology"
                , "com.qlangtech.tis.plugin.ontology.OntologyDomain"
                , "com.qlangtech.tis.plugin.ontology.OntologyGlossary"
                , "com.qlangtech.tis.plugin.ontology.OntologyLinker"
                , "com.qlangtech.tis.plugin.ontology.OntologyObjectType"
                , "com.qlangtech.tis.plugin.ontology.OntologySharedProperty"
                , "com.qlangtech.tis.plugin.ontology.OntologyValueType"
                , "com.qlangtech.tis.plugin.ontology.impl.OntologyPluginMeta"
                , "com.qlangtech.tis.plugin.ontology.impl.linker.LinkResources"
                , "com.qlangtech.tis.plugin.ontology.impl.synonyms.SynonymsElement");

        Collection<File> files = FileUtils.listFiles(new File("/Users/mozhenghua/j2ee_solution/project/tis-solr/tis-plugin/src/main/java/com/qlangtech/tis/plugin/ontology")
                , new String[]{"java"}, true);
        aa:
        for (File file : files) {
            String findFilePath = file.getAbsolutePath();

            for (String reserved : reservedFile) {
                if (StringUtils.endsWith(findFilePath, reserved.replace(".", "/") + ".java")) {
                    continue aa;
                }
            }
            System.out.println(findFilePath);
            FileUtils.deleteQuietly(file);
        }

    }
}
