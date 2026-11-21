package servidor;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * GerenciadorArquivos
 * Centraliza o acesso à pasta de armazenamento do servidor de arquivos UDP.
 * Política de armazenamento (retirada da instrução do projeto):
 *  - Pasta central única: "central/" na raiz do projeto, criada sob demanda.
 *  - Qualquer extensão é aceita (.txt, .pdf, .jpg, .png, .docx etc.).
 *  - Nome duplicado nunca é sobrescrito: o novo arquivo recebe um sufixo
 *    numérico antes da extensão (ex.: relatorio.pdf -> relatorio_1.pdf).
 *  - Caminhos enviados pelo cliente são descartados: somente o nome final do
 *    arquivo é utilizado, impedindo gravação fora da pasta central.
 * Esta classe usa apenas o JDK (java.nio.file), sem dependências externas.
 */
public class GerenciadorArquivos {

    /** Nome da pasta central, na raiz do projeto. */
    private static final String PASTA_CENTRAL_NOME = "central";

    /** Caminho absoluto e normalizado da pasta central. */
    private final Path PASTA_CENTRAL_CAMINHO;



    /**
     * Cria o gerenciador e prepara a pasta central.
     *
     * @throws IOException se a pasta central não puder ser criada
     *         (o servidor não deve iniciar sem o armazenamento disponível).
     */
    public GerenciadorArquivos () throws IOException {

        PASTA_CENTRAL_CAMINHO = Path.of(PASTA_CENTRAL_NOME).toAbsolutePath().normalize();

        Files.createDirectories(PASTA_CENTRAL_CAMINHO);
    }



    /**
     * Devolve o caminho da pasta central (útil para logs e diagnósticos).
     *
     * @return caminho absoluto normalizado da pasta central.
     */
    public Path getPastaCentralCaminho () {
        return PASTA_CENTRAL_CAMINHO;
    }



    /**
     * Lista os arquivos (somente arquivos regulares) existentes na pasta central.
     *
     * @return lista imutável e ordenada com os nomes dos arquivos.
     * @throws IOException se a pasta central não puder ser lida.
     */
    public List<String> listarArquivos() throws IOException {

        try (Stream<Path> conteudo = Files.list(PASTA_CENTRAL_CAMINHO)) {

            List<String> nomes = conteudo.filter(Files::isRegularFile)
              .map(caminho -> caminho.getFileName().toString())
               .sorted().collect(Collectors.toCollection(ArrayList::new));

            return Collections.unmodifiableList(nomes);
        }
    }



    /**
     * Verifica se um nome de arquivo já existe na pasta central.
     * Nomes inválidos (nulos, vazios, com caminho, ou que tentem escapar
     * da pasta central) são considerados inexistentes.
     *
     * @param nome nome ou caminho informado pelo cliente (só o nome final é usado).
     * @return {@code true} se existir um arquivo regular com esse nome.
     */
    public boolean existeArquivo (String nome) {

        if (nome == null || nome.isBlank()) return false;

        try { return Files.isRegularFile(resolverCaminhoSeguro(nome)); }

        catch (IllegalArgumentException | SecurityException e) {

            return false;
        }
    }



    /**
     * Salva um arquivo recebido de um cliente na pasta central, garantindo um nome único no disco.
     * Regras aplicadas:
     *  - Apenas o nome final do arquivo é usado; qualquer caminho do cliente é descartado.
     *  - Se o nome já existir, é gerado um sufixo numérico antes da extensão
     *    (foto.png -> foto_1.png -> foto_2.png ...).
     *  - O arquivo nunca é sobrescrito: a gravação usa CREATE_NEW e o método é
     *    synchronized, tornando a operação atômica dentro desta JVM.
     *
     * @param nome  nome ou caminho informado pelo cliente.
     * @param dados bytes do arquivo recebido, já completamente remontados pelo
     *              reordenador de fragmentos antes desta chamada.
     * @return o nome final gravado no disco (pode diferir do informado).
     * @throws IllegalArgumentException se o nome ou os dados forem inválidos.
     * @throws IOException              se a gravação falhar.
     */
    public synchronized String salvarArquivo (String nome, byte[] dados) throws IOException {

        if (dados == null) throw new IllegalArgumentException("Bytes do arquivo são nulos.");

        String nomeBase = extrairNomeArquivo(nome);

        String nomeFinal = gerarNomeUnico(nomeBase);

        Path caminho = resolverCaminhoSeguro(nomeFinal);

        // CREATE_NEW: se, por corrida, o arquivo já existir, lança exceção em vez de sobrescrever o conteúdo já armazenado.

        Files.write(caminho, dados, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);

        return nomeFinal;
    }



    /**
     * Abre um arquivo da pasta central para baixar.
     * Somente arquivos regulares localizados dentro da pasta central são aceitos.
     *
     * @param nome nome do arquivo desejado.
     * @return caminho resolvido do arquivo dentro da pasta central.
     * @throws FileNotFoundException se o arquivo não existir ou não for um arquivo regular.
     * @throws IOException           se houver erro de acesso.
     */
    public Path abrirParaBaixar (String nome) throws IOException {

        Path caminho = resolverCaminhoSeguro(nome);

        if (!Files.isRegularFile(caminho)) throw new FileNotFoundException("Arquivo \"" + nome + "\" inexistente na pasta central.");

        return caminho;
    }



    /**
     * Obtém o tamanho em bytes de um arquivo da pasta central.
     *
     * @param nome nome do arquivo.
     * @return tamanho em bytes.
     * @throws FileNotFoundException se o arquivo não existir.
     * @throws IOException           se houver erro de acesso.
     */
    public long obterTamanhoArquivo (String nome) throws IOException {

        return Files.size(abrirParaBaixar(nome));
    }



    /**
     * Gera um nome que ainda não existe na pasta central.
     * Nome livre -> devolvido sem alteração; nome ocupado -> sufixo numérico
     * antes da extensão, incrementando até encontrar um vago.
     */
    private String gerarNomeUnico (String nomeBase) {

        if (!Files.exists(resolverCaminhoSeguro(nomeBase))) return nomeBase;

        String[] partes = separarNomeExtensao(nomeBase);

        int contador = 1;

        String candidato;

        do {
            candidato = partes[0] + "_" + contador + partes[1];

            contador++;

        } while (Files.exists(resolverCaminhoSeguro(candidato)));

        return candidato;
    }



    /**
     * Separa o nome base da extensão no último ponto.
     * "arquivo.txt" -> ["arquivo", ".txt"]
     * "arquivo"     -> ["arquivo", ""]
     * ".escondido"  -> [".escondido", ""]
     */
    private String[] separarNomeExtensao (String nome) {

        int ponto = nome.lastIndexOf('.');

        if (ponto <= 0) return new String[] { nome, "" };

        return new String[] { nome.substring(0, ponto), nome.substring(ponto) };
    }



    /**
     * Extrai apenas o nome final de um nome/caminho fornecido pelo cliente.
     * Aceita separadores "/" e "\" e descarta todo o caminho anterior.
     * Rejeita nomes nulos, vazios, "." e "..".
     */
    private String extrairNomeArquivo (String nomeFornecido) {

        if (nomeFornecido == null) throw new IllegalArgumentException("Nome de arquivo nulo.");

        String nome = nomeFornecido.replace('\\', '/');

        int barra = nome.lastIndexOf('/');

        if (barra >= 0) nome = nome.substring(barra + 1);

        nome = nome.trim();

        if (nome.isEmpty() || nome.equals(".") || nome.equals("..")) throw new IllegalArgumentException("Nome de arquivo, \"" + nomeFornecido + "\", inválido.");

        return nome;
    }



    /**
     * Resolve um nome dentro da pasta central e garante que o caminho final
     * permaneça DENTRO dela (proteção contra path traversal).
     */
    private Path resolverCaminhoSeguro (String nome) {

        String nomeLimpo = extrairNomeArquivo(nome);

        Path caminho = PASTA_CENTRAL_CAMINHO.resolve(nomeLimpo).normalize();

        if (!caminho.startsWith(PASTA_CENTRAL_CAMINHO)) throw new SecurityException("Tentativa de acessar caminho fora da pasta central: " + nome);

        return caminho;
    }



    /**
     * Lê o conteúdo completo de um arquivo da pasta central.
     *
     * @param nome nome do arquivo desejado.
     * @return bytes do arquivo.
     * @throws FileNotFoundException se o arquivo não existir ou não for um arquivo regular.
     * @throws IOException           se houver erro de leitura.
     */
    public byte[] lerArquivo (String nome) throws IOException {

        return Files.readAllBytes(abrirParaBaixar(nome));
    }



    /**
     * Lista os arquivos no formato do protocolo (ver comum.Datagrama): um item
     * "nome;tamanho" por arquivo, com o tamanho em bytes.
     * Um arquivo removido entre a listagem e a consulta do tamanho sai da lista.
     *
     * @return itens "nome;tamanho" em ordem alfabética.
     * @throws IOException se a pasta central não puder ser lida.
     */
    public List<String> listarArquivosComTamanho () throws IOException {

        List<String> itens = new ArrayList<>();

        for (String nome : listarArquivos()) {

            try { itens.add(nome + ";" + obterTamanhoArquivo(nome)); }

            catch (IOException e) { /* removido durante a consulta: fora da lista */ }
        }

        return itens;
    }
}