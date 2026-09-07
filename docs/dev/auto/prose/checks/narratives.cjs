// Narrative-only acceptance for the families through Offline, excluding accepted Chunking.
// This does not certify stage/signature/return coverage or regenerate chapters.
// Run: node docs/dev/auto/prose/checks/narratives.cjs
const fs = require('node:fs');
const path = require('node:path');
const assert = require('node:assert/strict');
const repo = path.resolve(__dirname, '../../../../..');
const topics = {
    Fields: 'fields', Indexing: 'indexing', Inference: 'inference', Vectorization: 'vectorization',
    Scoring: 'scoring', Similarities: 'similarity/lexical', SearchDocuments: 'searching/search_docs',
    SearchFields: 'searching/search_fields', SearchSimilarity: 'searching/search_similarity', Offline: 'offline',
};
const read = p => fs.readFileSync(path.join(repo, p), 'utf8').replace(/\r\n/g, '\n');
function section(text, heading, preamble = false) {
    const parts = text.split(`\n## ${heading}\n`);
    assert.equal(parts.length, 2, `exactly one ${heading}`);
    return parts[1].split(preamble ? /^#{2,} /m : /^## /m)[0].trim();
}
const solution = text => section(text, 'Solution');
const preamble = text => section(text, 'Implementation', true);
const textBlocks = text => [...text.matchAll(/(?:~~~|```)text\n([\s\S]*?)\n(?:~~~|```)/g)].map(m => m[1]);
const formulas = text => [...text.matchAll(/^\$\$\n([\s\S]*?)\n\$\$$/gm)].map(m => m[1]);
const normalize = text => text.replace(/(?:~~~|```)text\n[\s\S]*?\n(?:~~~|```)/g, '<model>')
    .replace(/^\$\$\n[\s\S]*?\n\$\$$/gm, '<model>');
const prose = text => normalize(text).replace(/<model>/g, '').replace(/\\\[[\s\S]*?\\\]/g, '');
const words = text => prose(text).trim().split(/\s+/).filter(Boolean).length;
function validate(topic, docs, reference) {
    const minSolution = Math.ceil(words(solution(reference)) * 0.8);
    const minPreamble = Math.ceil(words(preamble(reference)) * 0.8);
    for (const [phase, doc] of Object.entries(docs)) {
        const sol = solution(doc), pre = preamble(doc);
        assert.ok(words(sol) >= minSolution, `${topic}/${phase}: Solution depth regression`);
        assert.ok(words(pre) >= minPreamble && pre.length > 0, `${topic}/${phase}: missing/compressed preamble`);
        assert.ok(prose(sol).trim().split(/\n\s*\n/).length >= 3, `${topic}/${phase}: developed Solution`);
        assert.ok(pre.split(/\n\s*\n/).length >= 3, `${topic}/${phase}: developed preamble`);
        assert.doesNotMatch(sol + pre, /collected source|numbering stream|prose pipeline|text operator/i);
        assert.ok((doc.match(/search engine/gi) || []).length <= 1, `${topic}/${phase}: phrase budget`);
    }
    assert.equal(solution(docs.draft), solution(docs.ext), `${topic}: Draft/Extend Solution parity`);
    assert.equal(normalize(solution(docs.ext)), normalize(solution(docs.form)), `${topic}: Form Solution parity`);
    assert.equal(preamble(docs.draft), preamble(docs.ext), `${topic}: Draft/Extend preamble parity`);
    assert.equal(preamble(docs.ext), preamble(docs.form), `${topic}: Form preamble parity`);
    assert.equal(section(docs.draft, 'Implementation'), preamble(docs.draft), `${topic}: Draft narrative only`);
    assert.equal(textBlocks(solution(docs.ext)).length, formulas(solution(docs.form)).length, `${topic}: model mapping`);
    assert.doesNotMatch(solution(docs.form), /~~~|```|\\\[|\\(?:small|tiny|footnotesize|scriptsize|resizebox|scalebox)\b/);
    const math = formulas(solution(docs.form));
    assert.equal((solution(docs.form).match(/^\$\$$/gm) || []).length, math.length * 2, `${topic}: balanced models`);
    assert.equal(section(docs.ext, 'Code'), section(docs.form, 'Code'), `${topic}: downstream Code parity`);
    if (topic === 'SearchFields') {
        for (const example of ['title:guide', 'aurora beacon', 'title:"release notes" and content:upgrade', 'meta:aurora']) {
            assert.ok(solution(docs.ext).includes(example), `${topic}: example ${example}`);
        }
        assert.match(preamble(docs.ext), /10,000/);
        assert.match(preamble(docs.ext), /does not rerank within the field-qualified population/);
    }
    if (topic === 'SearchDocuments') for (const limit of ['10,000', '1,000', '100 results']) {
        assert.ok(preamble(docs.ext).includes(limit), `${topic}: distinct limit ${limit}`);
    }
    if (topic === 'Offline') {
        assert.match(preamble(docs.ext), /three independent composed transforms/);
        assert.match(preamble(docs.ext), /bounds only the popular branch/);
        assert.match(preamble(docs.ext), /10,000/);
        assert.match(preamble(docs.ext), /zero through seven days/);
    }
}
const report = [];
let mutations = 0;
for (const [topic, dir] of Object.entries(topics)) {
    const docs = Object.fromEntries([['draft', 'draft'], ['extended', 'ext'], ['form', 'form']].map(([phase, suffix]) =>
        [suffix, read(`close/${phase}/search/transforms/${dir}/${topic}.${suffix}.md`)]));
    const reference = read(`close/3/extended/search/transforms/${dir}/${topic}.ext.md`);
    validate(topic, docs, reference);
    // Equal compression across all phases must fail, not merely a downstream mismatch.
    const shrunk = Object.fromEntries(Object.entries(docs).map(([phase, text]) =>
        [phase, text.replace(solution(text), 'Prepare reusable evidence for later retrieval.')]));
    assert.throws(() => validate(topic, shrunk, reference), /Solution depth regression/);
    const missing = Object.fromEntries(Object.entries(docs).map(([phase, text]) => [phase, text.replace(preamble(text), '')]));
    assert.throws(() => validate(topic, missing, reference), /missing\/compressed preamble/);
    const drifted = { ...docs, form: docs.form.replace(preamble(docs.form), preamble(docs.form) + '\n\nUnrequested rewrite.') };
    assert.throws(() => validate(topic, drifted, reference), /preamble parity/);
    mutations += 3;
    if (formulas(solution(docs.form)).length) {
        const textOnly = { ...docs, form: docs.form.replace(solution(docs.form), solution(docs.ext)) };
        assert.throws(() => validate(topic, textOnly, reference), /model mapping/);
        mutations++;
    }
    report.push({ topic, solution: words(solution(docs.ext)), referenceSolution: words(solution(reference)),
        preamble: words(preamble(docs.ext)), referencePreamble: words(preamble(reference)),
        solutionFormulas: formulas(solution(docs.form)).length });
}
console.log(JSON.stringify({ scope: 'Solution and Implementation preamble only', families: report, rejectedMutations: mutations }, null, 2));
