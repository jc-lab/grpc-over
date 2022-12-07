package kr.jclab.grpcover.gofprotocol;

public interface GofStreamVisitor {
    /**
     * @return <ul>
     *         <li>{@code true} if the visitor wants to continue the loop and handle the entry.</li>
     *         <li>{@code false} if the visitor wants to stop handling headers and abort the loop.</li>
     *         </ul>
     */
    boolean visit(GofStream stream) throws GofException;
}
