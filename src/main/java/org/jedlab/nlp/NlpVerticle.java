package org.jedlab.nlp;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpHeaders;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.fa.PersianAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.Version;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.html.HtmlParser;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.SAXException;

/**
 * @author Omid Pourhadi
 *
 */
public class NlpVerticle extends AbstractVerticle
{

    public static final String USER_HOME = System.getProperty("user.home");
    public static final String INDEX_REPOSITORY = USER_HOME + "/search_index";

    @Override
    public void start() throws Exception
    {
        vertx.executeBlocking(b ->
        {
            vertx.fileSystem().mkdirBlocking(INDEX_REPOSITORY);
        }, rh ->
        {
            System.out.println("directory already created : " + rh.succeeded());
        });
        Router router = Router.router(vertx);
        Future<HttpResponse<Buffer>> urlResponseFuture = Future.future();
        UrlContentHandler urlContentHandler = new UrlContentHandler(urlResponseFuture);
        WordsHandler wordsHandler = new WordsHandler();
        router.route("/parse").handler(urlContentHandler::handle);
        router.route("/find/:words").handler(wordsHandler::handle);
        vertx.createHttpServer().requestHandler(router::accept).listen(8585);
        //
        urlResponseFuture.setHandler(h ->
        {
            String content = h.result().bodyAsString();
            final ContentParser cp = new ContentParser(content);
            String parsedContent = cp.parse();
            LuceneManager lm = new LuceneManager();
            lm.index(parsedContent);
            System.out.println("process finished");
            urlResponseFuture.complete();
        });
    }

    public static class WordsHandler
    {

        public WordsHandler()
        {
        }

        private void handle(RoutingContext rc)
        {
            String words = rc.request().getParam("words");
            StringBuilder sb = new StringBuilder();
            if (words != null && words.length() > 0)
            {
                DirectoryReader reader = null;
                try

                {
                    Directory fsDir = FSDirectory.open(new File(INDEX_REPOSITORY));
                    reader = DirectoryReader.open(fsDir);
                    IndexSearcher searcher = new IndexSearcher(reader);
                    Analyzer stdAn = new PersianAnalyzer(Version.LUCENE_45);
                    QueryParser parser = new QueryParser(Version.LUCENE_45, "text", stdAn);
                    sb.append("<table border='0'>");
                    sb.append("<thead>");
                    sb.append("<tr>");
                    sb.append("<th>rank</th>");
                    sb.append("<th>score</th>");
                    sb.append("<th>word</th>");
                    sb.append("<th>hits</th>");
                    sb.append("</tr>");
                    sb.append("</thead>");
                    sb.append("<tbody>");
                    String[] split = words.split(",");
                    for (int i = 0; i < split.length; i++)
                    {
                        String word = split[i];
                        Query q = parser.parse(word);
                        TopDocs hits = searcher.search(q, 2);
                        ScoreDoc[] scoreDocs = hits.scoreDocs;
                        sb.append("<tr>");
                        for (int n = 0; n < scoreDocs.length; n++)
                        {
                            ScoreDoc sd = scoreDocs[n];
                            float score = sd.score;
                            int docId = sd.doc;

                            sb.append("<td>");
                            sb.append(Integer.valueOf(n));
                            sb.append("</td>");
                            sb.append("<td>");
                            sb.append(Float.valueOf(score));
                            sb.append("</td>");
                            sb.append("<td>");
                            sb.append(word);
                            sb.append("</td>");
                            sb.append("<td>");
                            sb.append(String.valueOf(scoreDocs.length));
                            sb.append("</td>");
                        }

                        sb.append("</tr>");
                    }

                    sb.append("</tbody>");
                    sb.append("</table>");
                    sb.append("<br/>");
                }
                catch (Exception e)
                {

                }
                finally
                {
                    if (reader != null)
                    {
                        try
                        {
                            reader.close();
                        }
                        catch (IOException e)
                        {
                            // DO NOTHING
                        }
                    }
                }
            }
            sb.append("<br />Process completed");
            rc.response().putHeader(HttpHeaders.CONTENT_TYPE, "text/html; charset=utf-8");
            rc.response().end(sb.toString());

        }

    }

    public static class LuceneManager
    {

        public void index(String content)
        {
            try
            {
                Analyzer analyzer = new PersianAnalyzer(Version.LUCENE_CURRENT);
                boolean recreateIndexIfExists = true;
                IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_CURRENT, analyzer);
                conf.setOpenMode(IndexWriterConfig.OpenMode.CREATE);

                IndexWriter indexWriter = new IndexWriter(new NIOFSDirectory(new File(INDEX_REPOSITORY)), conf);
                Document document = new Document();
                Reader reader = new StringReader(content);
                document.add(new TextField("text", reader));
                indexWriter.addDocument(document);

                reader.close();
                indexWriter.close();
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }
    }

    public static class ContentParser
    {

        private static final BodyContentHandler handler = new BodyContentHandler();
        private static final Metadata metadata = new Metadata();
        private static final ParseContext pcontext = new ParseContext();

        private static final HtmlParser htmlparser = new HtmlParser();
        private String content;

        public ContentParser(String content)
        {
            this.content = content;
        }

        public String parse()
        {

            try
            {
                htmlparser.parse(new ByteArrayInputStream(content.getBytes("UTF-8")), handler, metadata, pcontext);
                return handler.toString();
            }
            catch (IOException | SAXException | TikaException e)
            {
                System.out.println(e.getMessage());
            }
            return "";
        }

    }

    public static class UrlContentHandler
    {

        private final Future<HttpResponse<Buffer>> future;

        public UrlContentHandler(Future<HttpResponse<Buffer>> future)
        {
            this.future = future;
        }

        private void handle(RoutingContext rc)
        {
            String url = rc.request().getParam("url");
            System.out.println("fetching content from : " + url);
            if (url != null && url.length() > 0)
            {
                WebClientOptions options = new WebClientOptions().setUserAgent("Mozilla/5.0").setUserAgentEnabled(true);
                options.setKeepAlive(true);
                options.setFollowRedirects(true);
                final WebClient client = WebClient.create(rc.vertx(), options);
                client.getAbs(url).send(future.completer());
                // client.get(80, url,"/").send(ar->{
                // System.out.println(ar.result().bodyAsString());
                // });
            }
            rc.response().end("process completed");

        }

    }

    public static void main(String[] args)
    {
        Runner.runExample(NlpVerticle.class);
    }

}
